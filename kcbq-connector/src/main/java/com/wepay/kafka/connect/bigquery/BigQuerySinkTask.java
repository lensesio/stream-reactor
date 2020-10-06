/*
 * Copyright 2020 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
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

package com.wepay.kafka.connect.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;
import com.wepay.kafka.connect.bigquery.exception.SinkConfigConnectException;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.TopicToTableResolver;
import com.wepay.kafka.connect.bigquery.utils.Version;
import com.wepay.kafka.connect.bigquery.write.batch.KCBQThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriter;
import com.wepay.kafka.connect.bigquery.write.row.AdaptiveBigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.row.SimpleBigQueryWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SinkTask} used to translate Kafka Connect {@link SinkRecord SinkRecords} into BigQuery
 * {@link RowToInsert RowToInserts} and subsequently write them to BigQuery.
 */
public class BigQuerySinkTask extends SinkTask {
  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkTask.class);

  private final BigQuery testBigQuery;
  private SchemaRetriever schemaRetriever;
  private BigQueryWriter bigQueryWriter;
  private BigQuerySinkTaskConfig config;
  private RecordConverter<Map<String, Object>> recordConverter;
  private Map<String, TableId> topicsToBaseTableIds;
  private boolean useMessageTimeDatePartitioning;

  private TopicPartitionManager topicPartitionManager;

  private KCBQThreadPoolExecutor executor;
  private static final int EXECUTOR_SHUTDOWN_TIMEOUT_SEC = 30;

  public BigQuerySinkTask() {
    testBigQuery = null;
    schemaRetriever = null;
  }

  // For testing purposes only; will never be called by the Kafka Connect framework
  public BigQuerySinkTask(BigQuery testBigQuery, SchemaRetriever schemaRetriever) {
    this.testBigQuery = testBigQuery;
    this.schemaRetriever = schemaRetriever;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    try {
      executor.awaitCurrentTasks();
    } catch (InterruptedException err) {
      throw new ConnectException("Interrupted while waiting for write tasks to complete.", err);
    }

    topicPartitionManager.resumeAll();
  }

  private PartitionedTableId getRecordTable(SinkRecord record) {
    TableId baseTableId = topicsToBaseTableIds.get(record.topic());

    PartitionedTableId.Builder builder = new PartitionedTableId.Builder(baseTableId);
    if (useMessageTimeDatePartitioning) {
      if (record.timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
        throw new ConnectException("Message has no timestamp type, cannot use message timestamp to partition.");
      }

      builder.setDayPartition(record.timestamp());
    } else {
      builder.setDayPartitionForNow();
    }

    return builder.build();
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

  @Override
  public void put(Collection<SinkRecord> records) {
    logger.info("Putting {} records in the sink.", records.size());

    // create tableWriters
    Map<PartitionedTableId, TableWriter.Builder> tableWriterBuilders = new HashMap<>();

    for (SinkRecord record : records) {
      if (record.value() != null) {
        PartitionedTableId table = getRecordTable(record);
        if (schemaRetriever != null) {
          schemaRetriever.setLastSeenSchema(table.getBaseTableId(), record.topic(), record.valueSchema());
        }

        if (!tableWriterBuilders.containsKey(table)) {
          TableWriter.Builder tableWriterBuilder =
              new TableWriter.Builder(bigQueryWriter, table, record.topic());
          tableWriterBuilders.put(table, tableWriterBuilder);
        }
        tableWriterBuilders.get(table).addRow(getRecordRow(record));
      }
    }

    // add tableWriters to the executor work queue
    for (TableWriter.Builder builder : tableWriterBuilders.values()) {
      executor.execute(builder.build());
    }

    // check if we should pause topics
    long queueSoftLimit = config.getLong(BigQuerySinkTaskConfig.QUEUE_SIZE_CONFIG);
    if (queueSoftLimit != -1) {
      int currentQueueSize = executor.getQueue().size();
      if (currentQueueSize > queueSoftLimit) {
        topicPartitionManager.pauseAll();
      } else if (currentQueueSize <= queueSoftLimit / 2) {
        // resume only if there is a reasonable chance we won't immediately have to pause again.
        topicPartitionManager.resumeAll();
      }
    }
  }

  private RecordConverter<Map<String, Object>> getConverter() {
    return config.getRecordConverter();
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
    schemaRetriever = config.getSchemaRetriever();
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
                                        retryWait);
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

    bigQueryWriter = getBigQueryWriter();
    topicsToBaseTableIds = TopicToTableResolver.getTopicsToTables(config);
    recordConverter = getConverter();
    executor = new KCBQThreadPoolExecutor(config, new LinkedBlockingQueue<>());
    topicPartitionManager = new TopicPartitionManager();
    useMessageTimeDatePartitioning = config.getBoolean(config.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG);
  }

  @Override
  public void stop() {
    try {
      executor.shutdown();
      executor.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("{} active threads are still executing tasks {}s after shutdown is signaled.",
          executor.getActiveCount(), EXECUTOR_SHUTDOWN_TIMEOUT_SEC);
    } finally {
      logger.trace("task.stop()");
    }
  }

  @VisibleForTesting
  int getTaskThreadsActiveCount() {
    return executor.getActiveCount();
  }

  @Override
  public String version() {
    String version = Version.version();
    logger.trace("task.version() = {}", version);
    return version;
  }

  private class TopicPartitionManager {

    private Long lastChangeMs;
    private boolean isPaused;

    public TopicPartitionManager() {
      this.lastChangeMs = System.currentTimeMillis();
      this.isPaused = false;
    }

    public void pauseAll() {
      if (!isPaused) {
        long now = System.currentTimeMillis();
        logger.warn("Paused all partitions after {}ms", now - lastChangeMs);
        isPaused = true;
        lastChangeMs = now;
      }
      Set<TopicPartition> assignment = context.assignment();
      context.pause(assignment.toArray(new TopicPartition[assignment.size()]));
    }

    public void resumeAll() {
      if (isPaused) {
        long now = System.currentTimeMillis();
        logger.info("Resumed all partitions after {}ms", now - lastChangeMs);
        isPaused = false;
        lastChangeMs = now;
      }
      Set<TopicPartition> assignment = context.assignment();
      context.resume(assignment.toArray(new TopicPartition[assignment.size()]));
    }
  }
}
