package com.wepay.kafka.connect.bigquery;

/*
 * Copyright 2016 WePay, Inc.
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
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;

import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.SinkConfigConnectException;

import com.wepay.kafka.connect.bigquery.utils.MetricsConstants;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.TopicToTableResolver;
import com.wepay.kafka.connect.bigquery.utils.Version;

import com.wepay.kafka.connect.bigquery.write.batch.BatchWriter;

import com.wepay.kafka.connect.bigquery.write.row.AdaptiveBigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.row.SimpleBigQueryWriter;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * A {@link SinkTask} used to translate Kafka Connect {@link SinkRecord SinkRecords} into BigQuery
 * {@link RowToInsert RowToInserts} and subsequently write them to BigQuery.
 */
public class BigQuerySinkTask extends SinkTask {
  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkTask.class);

  private final ExecutorService executorService = Executors.newCachedThreadPool();

  private final Object contextLock = new Object();

  private final BigQuery testBigQuery;
  private BigQuerySinkTaskConfig config;
  private RecordConverter<Map<String, Object>> recordConverter;
  private Map<PartitionedTableId, List<RowToInsert>> tableBuffers;
  private Map<TableId, Set<Schema>> tableSchemas;
  private BatchWriterManager batchWriterManager;
  private Map<TableId, String> baseTableIdsToTopics;
  private Map<String, TableId> topicsToBaseTableIds;

  private TopicPartitionManager topicPartitionManager;

  private Metrics metrics;
  private Sensor rowsRead;

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
    private final PartitionedTableId partitionedTableId;
    private final List<RowToInsert> rows;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;
    private final String topic;
    private final Set<Schema> schemas;
    private final BatchWriter<RowToInsert> batchWriter;

    public TableWriter(PartitionedTableId partitionedTableId,
                       List<RowToInsert> rows,
                       Map<TopicPartition, OffsetAndMetadata> offsets,
                       String topic,
                       Set<Schema> schemas) {
      this.partitionedTableId = partitionedTableId;
      this.rows = rows;
      this.offsets = offsets;
      this.topic = topic;
      this.schemas = schemas;
      this.batchWriter = batchWriterManager.getBatchWriter(partitionedTableId.getBaseTableId());
    }

    @Override
    public Void call() throws InterruptedException {
      batchWriter.writeAll(partitionedTableId, rows, topic, schemas);
      updateAllPartitions(baseTableIdsToTopics.get(partitionedTableId.getBaseTableId()), offsets);
      return null;
    }
  }

  /**
   * A class for keeping track of the BatchWriters for each base table.
   */
  private static class BatchWriterManager {
    private final BigQueryWriter bigQueryWriter;
    private final Class<BatchWriter<InsertAllRequest.RowToInsert>> batchWriterClass;
    // map from base TableId to the batchWriter for that base table
    private Map<TableId, BatchWriter<RowToInsert>> baseTableBatchWriterMap;

    /**
     * @param bigQueryWriter the {@link BigQueryWriter} to use to call BigQuery.
     * @param batchWriterClass the class of the BatchWriter to use
     * @param numTables the number of tables we are expecting to write to.
     */
    public BatchWriterManager(BigQueryWriter bigQueryWriter,
                              Class<BatchWriter<InsertAllRequest.RowToInsert>> batchWriterClass,
                              int numTables) {
      this.bigQueryWriter = bigQueryWriter;
      this.batchWriterClass = batchWriterClass;
      baseTableBatchWriterMap = new HashMap<>(numTables);
    }

    public synchronized BatchWriter<RowToInsert> getBatchWriter(TableId baseTableId) {
      if (!baseTableBatchWriterMap.containsKey(baseTableId)) {
        addNewBatchWriter(baseTableId);
      }
      return baseTableBatchWriterMap.get(baseTableId);
    }

    private void addNewBatchWriter(TableId baseTableId) {
      BatchWriter<RowToInsert> batchWriter;
      try {
        batchWriter = batchWriterClass.newInstance();
        batchWriter.init(bigQueryWriter);
      } catch (InstantiationException
        | IllegalAccessException
        exception) {
        throw new ConfigException("Failed to instantiate class specified for BatchWriter",
          exception);
      }
      baseTableBatchWriterMap.put(baseTableId, batchWriter);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    List<TableWriter> tableWriters = new ArrayList<>();
    for (Map.Entry<PartitionedTableId, List<RowToInsert>> bufferEntry : tableBuffers.entrySet()) {
      PartitionedTableId table = bufferEntry.getKey();
      List<RowToInsert> buffer = bufferEntry.getValue();
      if (!buffer.isEmpty()) {
        tableWriters.add(
            new TableWriter(
                table,
                buffer,
                offsets,
                baseTableIdsToTopics.get(table.getBaseTableId()),
                tableSchemas.get(table.getBaseTableId())
            )
        );
      }
      tableSchemas.put(table.getBaseTableId(), new HashSet<>());
    }
    // now that we are done, wipe all buffers.
    tableBuffers.clear();
    if (!tableWriters.isEmpty()) {
      try {
        for (Future<Void> tableWriteResult : executorService.invokeAll(tableWriters)) {
          tableWriteResult.get();
        }
      } catch (InterruptedException err) {
        throw new ConnectException("Interrupted while scheduling/executing write threads", err);
      } catch (ExecutionException err) {
        throw new BigQueryConnectException("Exception occurred while executing write threads",
                                           err);
      }
    }
    topicPartitionManager.resumeAll();
  }

  private PartitionedTableId getRecordTable(SinkRecord record) {
    TableId baseTableId = topicsToBaseTableIds.get(record.topic());
    return new PartitionedTableId.Builder(baseTableId).setDayPartitionForNow().build();
  }

  private String getRowId(SinkRecord record) {
    return String.format("%s-%d-%d",
                         record.topic(),
                         record.kafkaPartition(),
                         record.kafkaOffset());
  }

  private RowToInsert getRecordRow(SinkRecord record) {
    return RowToInsert.of(
      getRowId(record),
      recordConverter.convertRecord(record)
    );
  }

  // method for initializing the "buffer" to the buffer size.
  private List<RowToInsert> getNewBuffer() {
    Long bufferSize = config.getLong(config.BUFFER_SIZE_CONFIG);
    if (bufferSize == -1) {
      return new LinkedList<>();
    } else {
      return new ArrayList<>(bufferSize.intValue());
    }
  }

  // return true if the given buffer size is larger than or equal to the configured buffer size.
  private boolean bufferFull(int currentBufferSize) {
    Long bufferSize = config.getLong(config.BUFFER_SIZE_CONFIG);
    if (bufferSize == -1) {
      return false;
    }
    return currentBufferSize >= bufferSize;
  }

  private Map<PartitionedTableId, List<SinkRecord>>
      getRecordsByTable(Collection<SinkRecord> records) {
    Map<PartitionedTableId, List<SinkRecord>> tableRecords = new HashMap<>();
    for (SinkRecord record : records) {
      if (recordEmpty(record)) {
        // ignore it
        logger.debug("ignoring empty record value for topic: " + record.topic());
        continue;
      }
      PartitionedTableId tableId = getRecordTable(record);
      if (!tableRecords.containsKey(tableId)) {
        tableRecords.put(tableId, new ArrayList<>());
      }
      tableRecords.get(tableId).add(record);
    }
    return tableRecords;
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    Map<PartitionedTableId, List<SinkRecord>> recordsMap = getRecordsByTable(records);
    for (Map.Entry<PartitionedTableId, List<SinkRecord>> tableRecords : recordsMap.entrySet()) {
      PartitionedTableId partitionedTableId = tableRecords.getKey();

      if (!tableBuffers.containsKey(partitionedTableId)) {
        tableBuffers.put(partitionedTableId, getNewBuffer());
      }

      if (!tableSchemas.containsKey(partitionedTableId.getBaseTableId())) {
        tableSchemas.put(partitionedTableId.getBaseTableId(), new HashSet<>());
      }

      List<RowToInsert> buffer = tableBuffers.get(partitionedTableId);
      Set<Schema> schemas = tableSchemas.get(partitionedTableId.getBaseTableId());

      List<RowToInsert> tableRows = new ArrayList<>();
      for (SinkRecord record : tableRecords.getValue()) {
        schemas.add(record.valueSchema());
        tableRows.add(getRecordRow(record));
      }

      buffer.addAll(tableRows);
      if (bufferFull(buffer.size())) {
        topicPartitionManager.pause(
            baseTableIdsToTopics.get(partitionedTableId.getBaseTableId()),
            buffer.size()
        );
      }
    }
    rowsRead.record(recordsMap.size());
  }

  /**
   * Returns true if the given {@link SinkRecord} contains no value.
   * @param record the {@link SinkRecord} to check.
   * @return true if the record has no value, false otherwise.
   */
  private boolean recordEmpty(SinkRecord record) {
    return record.value() == null;
  }

  private RecordConverter<Map<String, Object>> getConverter() {
    return config.getRecordConverter();
  }

  private BatchWriterManager getBatchWriterManager() {
    @SuppressWarnings("unchecked")
    Class<BatchWriter<InsertAllRequest.RowToInsert>> batchWriterClass =
        (Class<BatchWriter<RowToInsert>>) config.getClass(config.BATCH_WRITER_CONFIG);

    return new BatchWriterManager(getBigQueryWriter(), batchWriterClass, tableSchemas.size());
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
    SchemaRetriever schemaRetriever = config.getSchemaRetriever();
    SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter =
        config.getSchemaConverter();
    return new SchemaManager(schemaRetriever, schemaConverter, bigQuery);
  }

  private BigQueryWriter getBigQueryWriter() {
    boolean updateSchemas = config.getBoolean(config.SCHEMA_UPDATE_CONFIG);
    int retry = config.getInt(config.BIGQUERY_RETRY_CONFIG);
    long retryWait = config.getLong(config.BIGQUERY_RETRY_WAIT_CONFIG);
    BigQuery bigQuery = getBigQuery();
    if (updateSchemas) {
      return new AdaptiveBigQueryWriter(bigQuery,
                                        getSchemaManager(bigQuery),
                                        retry,
                                        retryWait,
                                        metrics);
    } else {
      return new SimpleBigQueryWriter(bigQuery, retry, retryWait, metrics);
    }
  }

  private void configureMetrics() {
    metrics = new Metrics();
    rowsRead = metrics.sensor("rows-read");
    rowsRead.add(metrics.metricName("rows-read-avg",
                                    MetricsConstants.groupName,
                                    "The average number of rows written per request"),
                 new Avg());
    rowsRead.add(metrics.metricName("rows-read-max",
                                    MetricsConstants.groupName,
                                    "The maximum number of rows written per request"),
                 new Max());
    rowsRead.add(metrics.metricName("rows-read-rate",
                                    MetricsConstants.groupName,
                                    "The average number of rows written per second"),
                 new Rate());
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

    configureMetrics();

    topicsToBaseTableIds = TopicToTableResolver.getTopicsToTables(config);
    baseTableIdsToTopics = TopicToTableResolver.getBaseTablesToTopics(config);
    recordConverter = getConverter();
    tableBuffers = new HashMap<>();
    tableSchemas = new HashMap<>();
    batchWriterManager = getBatchWriterManager();
    topicPartitionManager = new TopicPartitionManager();
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

  private enum State {
    PAUSED,
    RUNNING
  }

  private class TopicPartitionManager {

    private Map<TopicPartition, State> topicStates;
    private Map<TopicPartition, Long> topicChangeMs;

    public TopicPartitionManager() {
      topicStates = new HashMap<>();
      topicChangeMs = new HashMap<>();
    }

    public void pause(String topic, int topicBufferSize) {
      Long now = System.currentTimeMillis();
      Collection<TopicPartition> topicPartitions = getPartitionsForTopic(topic);
      long oldestChangeMs = now;
      for (TopicPartition topicPartition : topicPartitions) {
        if (topicChangeMs.containsKey(topicPartition)) {
          oldestChangeMs = Math.min(oldestChangeMs, topicChangeMs.get(topicPartition));
        }
        topicStates.put(topicPartition, State.PAUSED);
        topicChangeMs.put(topicPartition, now);
        context.pause(topicPartition);
      }

      logger.info("Paused all partitions for topic {} with buffer size {} after {}ms: [{}]",
                  topic,
                  topicBufferSize,
                  now - oldestChangeMs,
                  topicPartitionsString(topicPartitions));
    }

    public void resume(TopicPartition topicPartition) {
      Long now = System.currentTimeMillis();
      if (topicStates.containsKey(topicPartition)) {
        if (topicStates.get(topicPartition) == State.PAUSED) {
          logger.info("Restarting topicPartition {} from pause after {}ms",
                      topicPartition,
                      now - topicChangeMs.get(topicPartition));
          topicChangeMs.put(topicPartition, now);
        } else {
          logger.debug("'Restarting' already running partition {}",
                       topicPartition);
        }
      } else {
        logger.info("Restarting new topicPartition {}",
                    topicPartition);
        topicChangeMs.put(topicPartition, now);
      }
      topicStates.put(topicPartition, State.RUNNING);
      context.resume(topicPartition);
    }

    public void resumeAll() {
      for (Map.Entry<TopicPartition, State> topicState : topicStates.entrySet()) {
        resume(topicState.getKey());
      }
    }

    private Collection<TopicPartition> getPartitionsForTopic(String topic) {
      return context.assignment()
                    .stream()
                    .filter(topicPartition -> topicPartition.topic().equals(topic))
                    .collect(Collectors.toList());
    }

    private String topicPartitionsString(Collection<TopicPartition> topicPartitions) {
      List<String> topicPartitionStrings = topicPartitions.stream()
                                                          .map(TopicPartition::toString)
                                                          .collect(Collectors.toList());
      return String.join(", ", topicPartitionStrings);
    }
  }
}
