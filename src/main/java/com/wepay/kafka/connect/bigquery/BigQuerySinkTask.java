package com.wepay.kafka.connect.bigquery;

/*
 * Copyright 2016 Wepay, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.buffer.Buffer;
import com.wepay.kafka.connect.bigquery.buffer.EmptyBuffer;
import com.wepay.kafka.connect.bigquery.buffer.LimitedBuffer;
import com.wepay.kafka.connect.bigquery.buffer.UnlimitedBuffer;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;

import com.wepay.kafka.connect.bigquery.convert.RecordConverter;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.SinkConfigConnectException;

import com.wepay.kafka.connect.bigquery.partition.EqualPartitioner;
import com.wepay.kafka.connect.bigquery.partition.Partitioner;
import com.wepay.kafka.connect.bigquery.partition.SinglePartitioner;

import com.wepay.kafka.connect.bigquery.utils.Version;

import com.wepay.kafka.connect.bigquery.write.AdaptiveBigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.BigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.SimpleBigQueryWriter;

import io.confluent.connect.avro.AvroData;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A {@link SinkTask} used to translate Kafka Connect {@link SinkRecord SinkRecords} into BigQuery
 * {@link RowToInsert RowToInserts} and subsequently write them to BigQuery.
 */
public class BigQuerySinkTask extends SinkTask {
  public static final long TABLE_WRITE_INTERVAL = 1000L;

  private final ExecutorService executorService = Executors.newCachedThreadPool();

  private final Object contextLock = new Object();

  private final BigQuery testBigQuery;

  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkTask.class);
  private BigQuerySinkTaskConfig config;
  private RecordConverter<Map<String, Object>> recordConverter;
  private Map<TableId, Buffer<RowToInsert>> tableBuffers;
  private Partitioner<RowToInsert> rowPartitioner;
  private Map<String, String> topicsToDatasets;
  private Map<TableId, String> tablesToTopics;
  private BigQueryWriter bigQueryWriter;

  public BigQuerySinkTask() {
    testBigQuery = null;
  }

  // For testing purposes only; will never be called by the Kafka Connect framework
  BigQuerySinkTask(BigQuery testBigQuery) {
    this.testBigQuery = testBigQuery;
  }

  // Called asynchronously from TableWriter.call(); synchronization required on context
  private void updateAllPartitions(String topic, Map<TopicPartition, OffsetAndMetadata> offsets) {
    Map<TopicPartition, Long> topicOffsets = new HashMap<>();
    for (Map.Entry<TopicPartition, OffsetAndMetadata> topicPartitionOffset : offsets.entrySet()) {
      TopicPartition topicPartition = topicPartitionOffset.getKey();
      if (topicPartition.topic().equals(topic)) {
        topicOffsets.put(topicPartition, topicPartitionOffset.getValue().offset());
      }
    }
    if (!topicOffsets.isEmpty()) {
      synchronized (contextLock) {
        context.offset(topicOffsets);
      }
    }
  }

  private class TableWriter implements Callable<Void> {
    private final TableId table;
    private final List<RowToInsert> rows;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    public TableWriter(
        TableId table,
        List<RowToInsert> rows,
        Map<TopicPartition, OffsetAndMetadata> offsets) {
      this.table = table;
      this.rows = rows;
      this.offsets = offsets;
    }

    @Override
    public Void call() throws InterruptedException {
      List<List<RowToInsert>> partitions = rowPartitioner.partition(rows);
      bigQueryWriter.writeRows(table, partitions.get(0));
      for (List<RowToInsert> partition : partitions.subList(1, partitions.size())) {
        Thread.sleep(TABLE_WRITE_INTERVAL);
        bigQueryWriter.writeRows(table, partition);
      }
      updateAllPartitions(tablesToTopics.get(table), offsets);
      return null;
    }
  }

  // Called synchronously from flush(); no synchronization required on context
  private void resumeAllPartitions() {
    logger.debug("Resuming all partitions");
    for (TopicPartition topicPartition : context.assignment()) {
      context.resume(topicPartition);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    List<TableWriter> tableWriters = new ArrayList<>();
    for (Map.Entry<TableId, Buffer<RowToInsert>> bufferEntry : tableBuffers.entrySet()) {
      TableId table = bufferEntry.getKey();
      Buffer<RowToInsert> buffer = bufferEntry.getValue();
      if (buffer.hasAny()) {
        tableWriters.add(new TableWriter(table, buffer.getAll(), offsets));
      }
    }
    if (!tableWriters.isEmpty()) {
      try {
        for (Future<Void> tableWriteResult : executorService.invokeAll(tableWriters)) {
          tableWriteResult.get();
        }
      } catch (InterruptedException err) {
        throw new ConnectException("Interrupted while scheduling/executing write threads", err);
      } catch (ExecutionException err) {
        throw new BigQueryConnectException("Exception occurred while executing write threads", err);
      }
    }
    resumeAllPartitions();
  }

  private TableId getRecordTable(SinkRecord record) {
    String topic = record.topic();
    String dataset = topicsToDatasets.get(topic);
    String tableFromTopic = config.getTableFromTopic(topic);
    return TableId.of(dataset, tableFromTopic);
  }

  private String getRowId(SinkRecord record) {
    return String.format("%s-%d-%d", record.topic(), record.kafkaPartition(), record.kafkaOffset());
  }

  private RowToInsert getRecordRow(SinkRecord record) {
    return RowToInsert.of(
        getRowId(record),
        recordConverter.convertRecord(record)
    );
  }

  private Buffer<RowToInsert> getNewBuffer() {
    long bufferSize = config.getLong(config.BUFFER_SIZE_CONFIG);
    if (bufferSize == -1) {
      return new UnlimitedBuffer<>();
    } else if (bufferSize == 0) {
      return new EmptyBuffer<>();
    } else {
      return new LimitedBuffer<>(bufferSize);
    }
  }

  private Map<TableId, List<RowToInsert>> partitionRecordsByTable(
      Collection<SinkRecord> records) {
    Map<TableId, List<RowToInsert>> tableRows = new HashMap<>();
    for (SinkRecord record : records) {
      TableId tableId = getRecordTable(record);
      if (!tableRows.containsKey(tableId)) {
        tableRows.put(tableId, new ArrayList<>());
      }
      tableRows.get(tableId).add(getRecordRow(record));
    }
    return tableRows;
  }

  // Called synchronously in put(); no synchronization on context needed
  private void pauseAllPartitions(String topic) {
    logger.debug("Pausing all partitions for topic: " + topic);
    for (TopicPartition topicPartition : context.assignment()) {
      if (topicPartition.topic().equals(topic)) {
        context.pause(topicPartition);
      }
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (Map.Entry<TableId, List<RowToInsert>> tableRow
        : partitionRecordsByTable(records).entrySet()) {
      TableId table = tableRow.getKey();
      if (!tableBuffers.containsKey(table)) {
        tableBuffers.put(table, getNewBuffer());
      }
      Buffer<RowToInsert> buffer = tableBuffers.get(table);
      buffer.buffer(tableRow.getValue());
      if (buffer.hasExcess()) {
        pauseAllPartitions(tablesToTopics.get(table));
      }
    }
  }

  private RecordConverter<Map<String, Object>> getConverter() {
    return config.getRecordConverter();
  }

  private Partitioner<RowToInsert> getPartitioner() {
    int maxWriteSize = config.getInt(config.MAX_WRITE_CONFIG);
    if (maxWriteSize == -1) {
      return new SinglePartitioner<>();
    } else {
      return new EqualPartitioner<>(maxWriteSize);
    }
  }

  private BigQuery getBigQuery() {
    if (testBigQuery != null) {
      return testBigQuery;
    }
    String projectName = config.getString(config.PROJECT_CONFIG);
    String keyFilename = config.getString(config.KEYFILE_CONFIG);
    return new BigQueryHelper().connect(projectName, keyFilename);
  }

  private SchemaManager getSchemaManager(BigQuery bigQuery) {
    // Don't want to do ANY caching in the schema registry client, since we want to handle the
    // possibility of schemas changing during the lifetime of the connector
    return new SchemaManager(
        tablesToTopics,
        new CachedSchemaRegistryClient(config.getString(config.REGISTRY_CONFIG), 0),
        new org.apache.avro.Schema.Parser(),
        new AvroData(config.getInt(config.AVRO_DATA_CACHE_SIZE_CONFIG)),
        config.getSchemaConverter(),
        bigQuery
    );
  }

  private BigQueryWriter getWriter() {
    boolean updateSchemas = config.getBoolean(config.SCHEMA_UPDATE_CONFIG);
    int retry = config.getInt(config.BIGQUERY_RETRY_CONFIG);
    long retryWait = config.getLong(config.BIGQUERY_RETRY_WAIT_CONFIG);
    BigQuery bigQuery = getBigQuery();
    if (updateSchemas) {
      return new AdaptiveBigQueryWriter(bigQuery, getSchemaManager(bigQuery), retry, retryWait);
    } else {
      return new SimpleBigQueryWriter(bigQuery, retry, retryWait);
    }
  }

  @Override
  public void start(Map<String, String> properties) {
    logger.trace("task.start()");
    try {
      config = new BigQuerySinkTaskConfig(properties);
    } catch (ConfigException err) {
      throw new SinkConfigConnectException(
          "Couldn't start BigQuerySinkTask due to configuration error",
          err
      );
    }

    topicsToDatasets = config.getTopicsToDatasets();
    tablesToTopics = config.getTablesToTopics(topicsToDatasets);

    recordConverter = getConverter();
    tableBuffers = new HashMap<>();
    rowPartitioner = getPartitioner();
    bigQueryWriter = getWriter();
  }

  @Override
  public void stop() {
    logger.trace("task.stop()");
  }

  @Override
  public String version() {
    String version = Version.version();
    logger.trace("task.version() = {}", version);
    return version;
  }
}
