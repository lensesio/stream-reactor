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

package com.wepay.kafka.connect.bigquery.integration;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.NoRetryException;
import org.apache.kafka.test.TestUtils;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.cloud.bigquery.LegacySQLTypeName.BOOLEAN;
import static com.google.cloud.bigquery.LegacySQLTypeName.BYTES;
import static com.google.cloud.bigquery.LegacySQLTypeName.DATE;
import static com.google.cloud.bigquery.LegacySQLTypeName.FLOAT;
import static com.google.cloud.bigquery.LegacySQLTypeName.INTEGER;
import static com.google.cloud.bigquery.LegacySQLTypeName.STRING;
import static com.google.cloud.bigquery.LegacySQLTypeName.TIMESTAMP;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {
  private static final Logger logger = LoggerFactory.getLogger(BaseConnectorIT.class);

  private static final String KEYFILE_ENV_VAR = "KCBQ_TEST_KEYFILE";
  private static final String PROJECT_ENV_VAR = "KCBQ_TEST_PROJECT";
  private static final String DATASET_ENV_VAR = "KCBQ_TEST_DATASET";
  private static final String GCS_BUCKET_ENV_VAR = "KCBQ_TEST_BUCKET";
  private static final String GCS_FOLDER_ENV_VAR = "KCBQ_TEST_FOLDER";
  private static final String TEST_NAMESPACE_ENV_VAR = "KCBQ_TEST_TABLE_SUFFIX";

  protected static final long OFFSET_COMMIT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);
  protected static final long COMMIT_MAX_DURATION_MS = TimeUnit.MINUTES.toMillis(5);
  protected static final long OFFSETS_READ_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);

  protected EmbeddedConnectCluster connect;
  private Admin kafkaAdminClient;

  protected void startConnect() {
    Map<String, String> workerProps = new HashMap<>();
    workerProps.put(
        WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, Long.toString(OFFSET_COMMIT_INTERVAL_MS));
    // Allow per-connector consumer configuration for throughput testing
    workerProps.put(
        WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");

    connect = new EmbeddedConnectCluster.Builder()
        .name("kcbq-connect-cluster")
        .workerProps(workerProps)
        .build();

    // start the clusters
    connect.start();

    kafkaAdminClient = connect.kafka().createAdminClient();

    // the exception handler installed by the embedded zookeeper instance is noisy and unnecessary
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> { });
  }

  protected void stopConnect() {
    if (kafkaAdminClient !=  null) {
      Utils.closeQuietly(kafkaAdminClient, "admin client for embedded Kafka cluster");
      kafkaAdminClient = null;
    }

    // stop all Connect, Kafka and Zk threads.
    if (connect != null) {
      Utils.closeQuietly(connect::stop, "embedded Connect, Kafka, and Zookeeper clusters");
      connect = null;
    }
  }

  protected Map<String, String> baseConnectorProps(int tasksMax) {
    Map<String, String> result = new HashMap<>();

    result.put(CONNECTOR_CLASS_CONFIG, "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector");
    result.put(TASKS_MAX_CONFIG, Integer.toString(tasksMax));

    result.put(BigQuerySinkConfig.PROJECT_CONFIG, project());
    result.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset());
    result.put(BigQuerySinkConfig.KEYFILE_CONFIG, keyFile());
    result.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, keySource());

    result.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");

    return result;
  }

  protected BigQuery newBigQuery() {
    return new GcpClientBuilder.BigQueryBuilder()
        .withKey(keyFile())
        .withKeySource(GcpClientBuilder.KeySource.valueOf(keySource()))
        .withProject(project())
        .withUserAgent("ITTest-user-agent")
        .build();
  }

  protected void waitForCommittedRecords(
      String connector, String topic, long numRecords, int numTasks
  ) throws InterruptedException {
    waitForCommittedRecords(connector, Collections.singleton(topic), numRecords, numTasks, COMMIT_MAX_DURATION_MS);
  }

  protected void waitForCommittedRecords(
      String connector, Collection<String> topics, long numRecords, int numTasks, long timeoutMs
  ) throws InterruptedException {
    waitForCondition(
        () -> {
          long totalCommittedRecords = totalCommittedRecords(connector, topics);
          if (totalCommittedRecords >= numRecords) {
            return true;
          } else {
            // Check to make sure the connector is still running. If not, fail fast
            try {
              assertTrue(
                  "Connector or one of its tasks failed during testing",
                  assertConnectorAndTasksRunning(connector, numTasks).orElse(false));
            } catch (AssertionError e) {
              throw new NoRetryException(e);
            }
            logger.debug("Connector has only committed {} records for topics {} so far; {} expected",
                totalCommittedRecords, topics, numRecords);
            // Sleep here so as not to spam Kafka with list-offsets requests
            Thread.sleep(OFFSET_COMMIT_INTERVAL_MS / 2);
            return false;
          }
        },
        timeoutMs,
        "Either the connector failed, or the message commit duration expired without all expected messages committed");
  }

  protected synchronized long totalCommittedRecords(String connector, Collection<String> topics) throws TimeoutException, ExecutionException, InterruptedException {
    // See https://github.com/apache/kafka/blob/f7c38d83c727310f4b0678886ba410ae2fae9379/connect/runtime/src/main/java/org/apache/kafka/connect/util/SinkUtils.java
    // for how the consumer group ID is constructed for sink connectors
    Map<TopicPartition, OffsetAndMetadata> offsets = kafkaAdminClient
        .listConsumerGroupOffsets("connect-" + connector)
        .partitionsToOffsetAndMetadata()
        .get(OFFSETS_READ_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    logger.trace("Connector {} has so far committed offsets {}", connector, offsets);

    return offsets.entrySet().stream()
        .filter(entry -> topics.contains(entry.getKey().topic()))
        .mapToLong(entry -> entry.getValue().offset())
        .sum();
  }

  /**
   * Read all rows from the given table.
   * @param bigQuery used to connect to BigQuery
   * @param tableName the table to read
   * @param sortColumn a column to sort rows by (can use dot notation to refer to nested fields)
   * @return a list of all rows from the table, in random order.
   */
  protected List<List<Object>> readAllRows(
      BigQuery bigQuery, String tableName, String sortColumn) throws InterruptedException {

    Table table = bigQuery.getTable(dataset(), tableName);
    Schema schema = table
        .getDefinition()
        .getSchema();

    TableResult tableResult = bigQuery.query(QueryJobConfiguration.of(String.format(
        "SELECT * FROM `%s`.`%s` ORDER BY %s ASC",
        dataset(),
        tableName,
        sortColumn
    )));

    return StreamSupport.stream(tableResult.iterateAll().spliterator(), false)
        .map(fieldValues -> convertRow(schema.getFields(), fieldValues))
        .collect(Collectors.toList());
  }

  protected static List<Byte> boxByteArray(byte[] bytes) {
    Byte[] result = new Byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      result[i] = bytes[i];
    }
    return Arrays.asList(result);
  }

  private Object convertField(Field fieldSchema, FieldValue field) {
    if (field.isNull()) {
      return null;
    }
    switch (field.getAttribute()) {
      case PRIMITIVE:
        if (fieldSchema.getType().equals(BOOLEAN)) {
          return field.getBooleanValue();
        } else if (fieldSchema.getType().equals(BYTES)) {
          // Do this in order for assertEquals() to work when this is an element of two compared
          // lists
          return boxByteArray(field.getBytesValue());
        } else if (fieldSchema.getType().equals(DATE)) {
          DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
          return LocalDate.parse(field.getStringValue(), dateFormatter)
              .atStartOfDay(ZoneOffset.UTC)
              .toInstant()
              .toEpochMilli();
        } else if (fieldSchema.getType().equals(FLOAT)) {
          return field.getDoubleValue();
        } else if (fieldSchema.getType().equals(INTEGER)) {
          return field.getLongValue();
        } else if (fieldSchema.getType().equals(STRING)) {
          return field.getStringValue();
        } else if (fieldSchema.getType().equals(TIMESTAMP)) {
          return field.getTimestampValue();
        } else {
          throw new RuntimeException("Cannot convert primitive field type "
              + fieldSchema.getType());
        }
      case REPEATED:
        List<Object> result = new ArrayList<>();
        for (FieldValue arrayField : field.getRepeatedValue()) {
          result.add(convertField(fieldSchema, arrayField));
        }
        return result;
      case RECORD:
        List<Field> recordSchemas = fieldSchema.getSubFields();
        List<FieldValue> recordFields = field.getRecordValue();
        return convertRow(recordSchemas, recordFields);
      default:
        throw new RuntimeException("Unknown field attribute: " + field.getAttribute());
    }
  }

  private List<Object> convertRow(List<Field> rowSchema, List<FieldValue> row) {
    List<Object> result = new ArrayList<>();
    assert (rowSchema.size() == row.size());

    for (int i = 0; i < rowSchema.size(); i++) {
      result.add(convertField(rowSchema.get(i), row.get(i)));
    }

    return result;
  }

  /**
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the given
   * name to start the specified number of tasks.
   *
   * @param name the name of the connector
   * @param numTasks the minimum number of tasks that are expected
   * @return the time this method discovered the connector has started, in milliseconds past epoch
   * @throws InterruptedException if this was interrupted
   */
  protected void waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
    waitForCondition(
        () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector tasks did not start in time."
    );
  }

  /**
   * Confirm that a connector with an exact number of tasks is running.
   *
   * @param connectorName the connector
   * @param numTasks the minimum number of tasks
   * @return true if the connector and tasks are in RUNNING state; false otherwise
   */
  protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      boolean result = info != null
                       && info.tasks().size() >= numTasks
                       && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
                       && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      return Optional.of(result);
    } catch (Exception e) {
      logger.debug("Could not check connector state info.", e);
      return Optional.empty();
    }
  }

  protected String suffixedTableOrTopic(String tableOrTopic) {
    return tableOrTopic + tableSuffix();
  }

  protected String sanitizedTable(String table) {
    return FieldNameSanitizer.sanitizeName(table);
  }

  protected String suffixedAndSanitizedTable(String table) {
    return sanitizedTable(suffixedTableOrTopic(table));
  }

  private String readEnvVar(String var) {
    String result = System.getenv(var);
    if (result == null) {
      throw new IllegalStateException(String.format(
          "Environment variable '%s' must be supplied to run integration tests",
          var));
    }
    return result;
  }

  private String readEnvVar(String var, String defaultVal) {
    return System.getenv().getOrDefault(var, defaultVal);
  }

  protected String keyFile() {
    return readEnvVar(KEYFILE_ENV_VAR);
  }

  protected String project() {
    return readEnvVar(PROJECT_ENV_VAR);
  }

  protected String dataset() {
    return readEnvVar(DATASET_ENV_VAR);
  }

  protected String keySource() {
    return BigQuerySinkConfig.KEY_SOURCE_DEFAULT;
  }

  protected String gcsBucket() {
    return readEnvVar(GCS_BUCKET_ENV_VAR);
  }

  protected String gcsFolder() {
    return readEnvVar(GCS_FOLDER_ENV_VAR, BigQuerySinkConfig.GCS_FOLDER_NAME_DEFAULT);
  }

  protected String tableSuffix() {
    return readEnvVar(TEST_NAMESPACE_ENV_VAR, "");
  }
}
