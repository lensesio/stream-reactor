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
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.TimePartitioning.Type;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.exception.SinkConfigConnectException;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.Version;
import com.wepay.kafka.connect.bigquery.write.batch.GCSBatchTableWriter;
import com.wepay.kafka.connect.bigquery.write.batch.KCBQThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriter;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import com.wepay.kafka.connect.bigquery.write.row.AdaptiveBigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.row.GCSToBQWriter;
import com.wepay.kafka.connect.bigquery.write.row.SimpleBigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.row.UpsertDeleteBigQueryWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.wepay.kafka.connect.bigquery.utils.TableNameUtils.intTable;

/**
 * A {@link SinkTask} used to translate Kafka Connect {@link SinkRecord SinkRecords} into BigQuery
 * {@link RowToInsert RowToInserts} and subsequently write them to BigQuery.
 */
public class BigQuerySinkTask extends SinkTask {
  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkTask.class);

  private AtomicReference<BigQuery> bigQuery;
  private AtomicReference<SchemaManager> schemaManager;
  private SchemaRetriever schemaRetriever;
  private BigQueryWriter bigQueryWriter;
  private GCSToBQWriter gcsToBQWriter;
  private BigQuerySinkTaskConfig config;
  private SinkRecordConverter recordConverter;

  private boolean useMessageTimeDatePartitioning;
  private boolean usePartitionDecorator;
  private boolean sanitize;
  private boolean upsertDelete;
  private MergeBatches mergeBatches;
  private MergeQueries mergeQueries;
  private volatile boolean stopped;

  private TopicPartitionManager topicPartitionManager;

  private KCBQThreadPoolExecutor executor;
  private static final int EXECUTOR_SHUTDOWN_TIMEOUT_SEC = 30;
  
  private final BigQuery testBigQuery;
  private final Storage testGcs;
  private final SchemaManager testSchemaManager;

  private final UUID uuid = UUID.randomUUID();
  private ScheduledExecutorService loadExecutor;

  private Map<TableId, Table> cache;

  /**
   * Create a new BigquerySinkTask.
   */
  public BigQuerySinkTask() {
    testBigQuery = null;
    schemaRetriever = null;
    testGcs = null;
    testSchemaManager = null;
  }

  /**
   * For testing purposes only; will never be called by the Kafka Connect framework.
   *
   * @param testBigQuery {@link BigQuery} to use for testing (likely a mock)
   * @param schemaRetriever {@link SchemaRetriever} to use for testing (likely a mock)
   * @param testGcs {@link Storage} to use for testing (likely a mock)
   * @param testSchemaManager {@link SchemaManager} to use for testing (likely a mock)
   * @see BigQuerySinkTask#BigQuerySinkTask()
   */
  public BigQuerySinkTask(BigQuery testBigQuery, SchemaRetriever schemaRetriever, Storage testGcs,
                          SchemaManager testSchemaManager, Map testCache) {
    this.testBigQuery = testBigQuery;
    this.schemaRetriever = schemaRetriever;
    this.testGcs = testGcs;
    this.testSchemaManager = testSchemaManager;
    this.cache = testCache;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    if (upsertDelete) {
      throw new ConnectException("This connector cannot perform upsert/delete on older versions of "
          + "the Connect framework; please upgrade to version 0.10.2.0 or later");
    }

    // Return immediately here since the executor will already be shutdown
    if (stopped) {
      // Still have to check for errors in order to prevent offsets being committed for records that
      // we've failed to write
      executor.maybeThrowEncounteredErrors();
      return;
    }

    try {
      executor.awaitCurrentTasks();
    } catch (InterruptedException err) {
      throw new ConnectException("Interrupted while waiting for write tasks to complete.", err);
    }

    topicPartitionManager.resumeAll();
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    if (upsertDelete) {
      Map<TopicPartition, OffsetAndMetadata> result = mergeBatches.latestOffsets();
      checkQueueSize();
      return result;
    }

    flush(offsets);
    return offsets;
  }

  private PartitionedTableId getRecordTable(SinkRecord record) {
    String tableName;
    String dataset = config.getString(config.DEFAULT_DATASET_CONFIG);
    String[] smtReplacement = record.topic().split(":");

    if (smtReplacement.length == 2) {
      dataset = smtReplacement[0];
      tableName = smtReplacement[1];
    } else if (smtReplacement.length == 1) {
      tableName = smtReplacement[0];
    } else {
      throw new ConfigException("Incorrect regex replacement format. " +
              "SMT replacement should either produce the <dataset>:<tableName> format or just the <tableName> format.");
    }

    if (sanitize) {
      tableName = FieldNameSanitizer.sanitizeName(tableName);
    }
    TableId baseTableId = TableId.of(dataset, tableName);
    if (upsertDelete) {
      TableId intermediateTableId = mergeBatches.intermediateTableFor(baseTableId);
      // If upsert/delete is enabled, we want to stream into a non-partitioned intermediate table
      return new PartitionedTableId.Builder(intermediateTableId).build();
    }

    PartitionedTableId.Builder builder = new PartitionedTableId.Builder(baseTableId);
    if (usePartitionDecorator) {
      Table bigQueryTable = retrieveCachedTable(baseTableId);
      TimePartitioning timePartitioning = TimePartitioning.of(Type.DAY);
      if (bigQueryTable != null) {
        StandardTableDefinition standardTableDefinition = bigQueryTable.getDefinition();
        if (standardTableDefinition != null && standardTableDefinition.getTimePartitioning() != null) {
          timePartitioning = standardTableDefinition.getTimePartitioning();
        }
      }

      if (useMessageTimeDatePartitioning) {
        if (record.timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
          throw new ConnectException(
              "Message has no timestamp type, cannot use message timestamp to partition.");
        }
        setTimePartitioningForTimestamp(builder, timePartitioning, record.timestamp());
      } else {
        setTimePartitioning(builder, timePartitioning);
      }
    }

    return builder.build();
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    // Periodically poll for errors here instead of doing a stop-the-world check in flush()
    executor.maybeThrowEncounteredErrors();

    logger.debug("Putting {} records in the sink.", records.size());

    // create tableWriters
    Map<PartitionedTableId, TableWriterBuilder> tableWriterBuilders = new HashMap<>();

    for (SinkRecord record : records) {
      if (record.value() != null || config.getBoolean(config.DELETE_ENABLED_CONFIG)) {
        PartitionedTableId table = getRecordTable(record);
        if (!tableWriterBuilders.containsKey(table)) {
          TableWriterBuilder tableWriterBuilder;
          if (config.getList(config.ENABLE_BATCH_CONFIG).contains(record.topic())) {
            String topic = record.topic();
            String gcsBlobName = topic + "_" + uuid + "_" + Instant.now().toEpochMilli();
            String gcsFolderName = config.getString(config.GCS_FOLDER_NAME_CONFIG);
            if (gcsFolderName != null && !"".equals(gcsFolderName)) {
              gcsBlobName = gcsFolderName + "/" + gcsBlobName;
            }
            tableWriterBuilder = new GCSBatchTableWriter.Builder(
                gcsToBQWriter,
                table.getBaseTableId(),
                config.getString(config.GCS_BUCKET_NAME_CONFIG),
                gcsBlobName,
                recordConverter);
          } else {
            TableWriter.Builder simpleTableWriterBuilder =
                new TableWriter.Builder(bigQueryWriter, table, recordConverter);
            if (upsertDelete) {
              simpleTableWriterBuilder.onFinish(rows ->
                  mergeBatches.onRowWrites(table.getBaseTableId(), rows));
            }
            tableWriterBuilder = simpleTableWriterBuilder;
          }
          tableWriterBuilders.put(table, tableWriterBuilder);
        }
        tableWriterBuilders.get(table).addRow(record, table.getBaseTableId());
      }
    }

    // add tableWriters to the executor work queue
    for (TableWriterBuilder builder : tableWriterBuilders.values()) {
      executor.execute(builder.build());
    }

    // check if we should pause topics
    checkQueueSize();
  }

  // Important: this method is only safe to call during put(), flush(), or preCommit(); otherwise,
  // a ConcurrentModificationException may be triggered if the Connect framework is in the middle of
  // a method invocation on the consumer for this task. This becomes especially likely if all topics
  // have been paused as the framework will most likely be in the middle of a poll for that consumer
  // which, because all of its topics have been paused, will not return until it's time for the next
  // offset commit. Invoking context.requestCommit() won't wake up the consumer in that case, so we
  // really have no choice but to wait for the framework to call a method on this task that implies
  // that it's safe to pause or resume partitions on the consumer.
  private void checkQueueSize() {
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

  private BigQuery getBigQuery() {
    if (testBigQuery != null) {
      return testBigQuery;
    }
    return bigQuery.updateAndGet(bq -> bq != null ? bq : newBigQuery());
  }

  private void setTimePartitioningForTimestamp(PartitionedTableId.Builder builder, TimePartitioning timePartitioning,
                                               Long timestamp) {
    switch (timePartitioning.getType()) {
      case HOUR:
        builder.setHourPartition(timestamp);
        break;
      case MONTH:
        builder.setMonthPartition(timestamp);
        break;
      case YEAR:
        builder.setYearPartition(timestamp);
        break;
      default:
        builder.setDayPartition(timestamp);
    }
  }

  private void setTimePartitioning(PartitionedTableId.Builder builder, TimePartitioning timePartitioning) {
    switch (timePartitioning.getType()) {
      case HOUR:
        builder.setHourPartitionNow();
        break;
      case MONTH:
        builder.setMonthPartitionForNow();
        break;
      case YEAR:
        builder.setYearPartitionForNow();
        break;
      default:
        builder.setDayPartitionForNow();
    }
  }

  private Table retrieveCachedTable(TableId tableId) {
    return getCache().computeIfAbsent(tableId, k -> getBigQuery().getTable(tableId));
  }

  private BigQuery newBigQuery() {
    String projectName = config.getString(config.PROJECT_CONFIG);
    String keyFile = config.getKeyFile();
    String keySource = config.getString(config.KEY_SOURCE_CONFIG);
    return new BigQueryHelper().setKeySource(keySource).connect(projectName, keyFile);
  }

  private SchemaManager getSchemaManager() {
    if (testSchemaManager != null) {
      return testSchemaManager;
    }
    return schemaManager.updateAndGet(sm -> sm != null ? sm : newSchemaManager());
  }

  private SchemaManager newSchemaManager() {
    schemaRetriever = config.getSchemaRetriever();
    SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter =
        config.getSchemaConverter();
    Optional<String> kafkaKeyFieldName = config.getKafkaKeyFieldName();
    Optional<String> kafkaDataFieldName = config.getKafkaDataFieldName();
    Optional<String> timestampPartitionFieldName = config.getTimestampPartitionFieldName();
    Optional<Long> partitionExpiration = config.getPartitionExpirationMs();
    Optional<List<String>> clusteringFieldName = config.getClusteringPartitionFieldName();
    Optional<TimePartitioning.Type> timePartitioningType = config.getTimePartitioningType();
    boolean allowNewBQFields = config.getBoolean(config.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG);
    boolean allowReqFieldRelaxation = config.getBoolean(config.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG);
    boolean allowSchemaUnionization = config.getBoolean(config.ALLOW_SCHEMA_UNIONIZATION_CONFIG);
    return new SchemaManager(schemaRetriever, schemaConverter, getBigQuery(),
                             allowNewBQFields, allowReqFieldRelaxation, allowSchemaUnionization,
                             kafkaKeyFieldName, kafkaDataFieldName,
                             timestampPartitionFieldName, partitionExpiration, clusteringFieldName, timePartitioningType);
  }

  private BigQueryWriter getBigQueryWriter() {
    boolean autoCreateTables = config.getBoolean(config.TABLE_CREATE_CONFIG);
    boolean allowNewBigQueryFields = config.getBoolean(config.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG);
    boolean allowRequiredFieldRelaxation = config.getBoolean(config.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG);
    int retry = config.getInt(config.BIGQUERY_RETRY_CONFIG);
    long retryWait = config.getLong(config.BIGQUERY_RETRY_WAIT_CONFIG);
    BigQuery bigQuery = getBigQuery();
    if (upsertDelete) {
      return new UpsertDeleteBigQueryWriter(bigQuery,
                                            getSchemaManager(),
                                            retry,
                                            retryWait,
                                            autoCreateTables,
                                            mergeBatches.intermediateToDestinationTables());
    } else if (autoCreateTables || allowNewBigQueryFields || allowRequiredFieldRelaxation) {
      return new AdaptiveBigQueryWriter(bigQuery,
                                        getSchemaManager(),
                                        retry,
                                        retryWait,
                                        autoCreateTables);
    } else {
      return new SimpleBigQueryWriter(bigQuery, retry, retryWait);
    }
  }

  private Storage getGcs() {
    if (testGcs != null) {
      return testGcs;
    }
    String projectName = config.getString(config.PROJECT_CONFIG);
    String key = config.getKeyFile();
    String keySource = config.getString(config.KEY_SOURCE_CONFIG);
    return new GCSBuilder(projectName).setKey(key).setKeySource(keySource).build();

  }

  private GCSToBQWriter getGcsWriter() {
    BigQuery bigQuery = getBigQuery();
    int retry = config.getInt(config.BIGQUERY_RETRY_CONFIG);
    long retryWait = config.getLong(config.BIGQUERY_RETRY_WAIT_CONFIG);
    boolean autoCreateTables = config.getBoolean(config.TABLE_CREATE_CONFIG);
    // schemaManager shall only be needed for creating table hence do not fetch instance if not
    // needed.
    SchemaManager schemaManager = autoCreateTables ? getSchemaManager() : null;
    return new GCSToBQWriter(getGcs(),
                         bigQuery,
                         schemaManager,
                         retry,
                         retryWait,
                         autoCreateTables);
  }

  private SinkRecordConverter getConverter(BigQuerySinkTaskConfig config) {
    return new SinkRecordConverter(config, mergeBatches, mergeQueries);
  }

  private synchronized Map<TableId, Table> getCache() {
    if (cache == null) {
      cache = new HashMap<>();
    }

    return cache;
  }

  @Override
  public void start(Map<String, String> properties) {
    logger.trace("task.start()");
    stopped = false;

    final boolean hasGCSBQTask =
        properties.remove(BigQuerySinkConnector.GCS_BQ_TASK_CONFIG_KEY) != null;
    try {
      config = new BigQuerySinkTaskConfig(properties);
    } catch (ConfigException err) {
      throw new SinkConfigConnectException(
          "Couldn't start BigQuerySinkTask due to configuration error",
          err
      );
    }
    upsertDelete = config.getBoolean(config.UPSERT_ENABLED_CONFIG)
        || config.getBoolean(config.DELETE_ENABLED_CONFIG);

    bigQuery = new AtomicReference<>();
    schemaManager = new AtomicReference<>();

    if (upsertDelete) {
      String intermediateTableSuffix = String.format("_%s_%d_%s_%d",
          config.getString(config.INTERMEDIATE_TABLE_SUFFIX_CONFIG),
          config.getInt(config.TASK_ID_CONFIG),
          uuid,
          Instant.now().toEpochMilli()
      );
      mergeBatches = new MergeBatches(intermediateTableSuffix);
    }

    cache = getCache();
    bigQueryWriter = getBigQueryWriter();
    gcsToBQWriter = getGcsWriter();
    executor = new KCBQThreadPoolExecutor(config, new LinkedBlockingQueue<>());
    topicPartitionManager = new TopicPartitionManager();
    useMessageTimeDatePartitioning =
        config.getBoolean(config.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG);
    usePartitionDecorator = 
            config.getBoolean(config.BIGQUERY_PARTITION_DECORATOR_CONFIG);
    sanitize =
            config.getBoolean(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG);
    if (hasGCSBQTask) {
      startGCSToBQLoadTask();
    } else if (upsertDelete) {
      mergeQueries =
          new MergeQueries(config, mergeBatches, executor, getBigQuery(), getSchemaManager(), context);
      maybeStartMergeFlushTask();
    }

    recordConverter = getConverter(config);
  }

  private void startGCSToBQLoadTask() {
    logger.info("Attempting to start GCS Load Executor.");
    loadExecutor = Executors.newScheduledThreadPool(1);
    String bucketName = config.getString(config.GCS_BUCKET_NAME_CONFIG);
    Storage gcs = getGcs();
    // get the bucket, or create it if it does not exist.
    Bucket bucket = gcs.get(bucketName);
    if (bucket == null) {
      // todo here is where we /could/ set a retention policy for the bucket,
      // but for now I don't think we want to do that.
      if (config.getBoolean(config.AUTO_CREATE_BUCKET_CONFIG)) {
        BucketInfo bucketInfo = BucketInfo.of(bucketName);
        bucket = gcs.create(bucketInfo);
      } else {
        throw new ConnectException("Bucket '" + bucketName + "' does not exist; Create the bucket manually, or set '" + config.AUTO_CREATE_BUCKET_CONFIG + "' to true");
      }
    }
    GCSToBQLoadRunnable loadRunnable = new GCSToBQLoadRunnable(getBigQuery(), bucket);

    int intervalSec = config.getInt(BigQuerySinkConfig.BATCH_LOAD_INTERVAL_SEC_CONFIG);
    loadExecutor.scheduleAtFixedRate(loadRunnable, intervalSec, intervalSec, TimeUnit.SECONDS);
  }

  private void maybeStartMergeFlushTask() {
    long intervalMs = config.getLong(config.MERGE_INTERVAL_MS_CONFIG);
    if (intervalMs == -1) {
      logger.info("{} is set to -1; periodic merge flushes are disabled", config.MERGE_INTERVAL_MS_CONFIG);
      return;
    }
    logger.info("Attempting to start upsert/delete load executor");
    loadExecutor = Executors.newScheduledThreadPool(1);
    loadExecutor.scheduleAtFixedRate(
        mergeQueries::mergeFlushAll, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    try {
      maybeStopExecutor(loadExecutor, "load executor");
      maybeStopExecutor(executor, "table write executor");
      if (upsertDelete) {
        mergeBatches.intermediateTables().forEach(table -> {
          logger.debug("Deleting {}", intTable(table));
          getBigQuery().delete(table);
        });
      }
    } finally {
      stopped = true;
    }

    logger.trace("task.stop()");
  }

  private void maybeStopExecutor(ExecutorService executor, String executorName) {
    if (executor == null) {
      return;
    }

    try {
      if (upsertDelete) {
        logger.trace("Forcibly shutting down {}", executorName);
        executor.shutdownNow();
      } else {
        logger.trace("Requesting shutdown for {}", executorName);
        executor.shutdown();
      }
      logger.trace("Awaiting termination of {}", executorName);
      executor.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
      logger.trace("Shut down {} successfully", executorName);
    } catch (Exception e) {
      logger.warn("Failed to shut down {}", executorName, e);
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
