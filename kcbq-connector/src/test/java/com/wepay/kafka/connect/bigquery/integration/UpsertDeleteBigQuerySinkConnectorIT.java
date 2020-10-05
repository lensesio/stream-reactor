package com.wepay.kafka.connect.bigquery.integration;

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
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class UpsertDeleteBigQuerySinkConnectorIT extends BaseConnectorIT {

  private static final Logger logger = LoggerFactory.getLogger(UpsertDeleteBigQuerySinkConnectorIT.class);

  private static final String CONNECTOR_NAME = "kcbq-sink-connector";
  private static final long NUM_RECORDS_PRODUCED = 20;
  private static final int TASKS_MAX = 3;
  private static final String KAFKA_FIELD_NAME = "kafkaKey";

  private BigQuery bigQuery;

  @Before
  public void setup() {
    bigQuery = newBigQuery();
    startConnect();
  }

  @After
  public void close() {
    bigQuery = null;
    stopConnect();
  }

  private Map<String, String> upsertDeleteProps(
      boolean upsert,
      boolean delete,
      long mergeRecordsThreshold) {
    if (!upsert && !delete) {
      throw new IllegalArgumentException("At least one of upsert or delete must be enabled");
    }

    Map<String, String> result = new HashMap<>();

    // use the JSON converter with schemas enabled
    result.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    result.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    if (upsert) {
      result.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    }
    if (delete) {
      result.put(BigQuerySinkConfig.DELETE_ENABLED_CONFIG, "true");
    }

    // Hardcode merge flushes to just use number of records for now, as it's more deterministic and
    // faster to test
    result.put(BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG, "-1");
    result.put(BigQuerySinkConfig.MERGE_RECORDS_THRESHOLD_CONFIG, Long.toString(mergeRecordsThreshold));

    result.put(BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG, KAFKA_FIELD_NAME);

    return result;
  }

  @Test
  public void testUpsert() throws Throwable {
    // create topic in Kafka
    final String topic = "test-upsert";
    // Make sure each task gets to read from at least one partition
    connect.kafka().createTopic(topic, TASKS_MAX);

    final String table = "test_upsert";
    clearPriorTable(bigQuery, table);

    // setup props for the sink connector
    Map<String, String> props = baseConnectorProps(TASKS_MAX);
    props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);

    props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    props.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");

    // Enable only upsert and not delete, and merge flush every other record
    props.putAll(upsertDeleteProps(true, false, 2));

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // Instantiate the converters we'll use to send records to the connector
    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // Send records to Kafka
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      // Each pair of records will share a key. Only the second record of each pair should be
      // present in the table at the end of the test
      String kafkaKey = key(keyConverter, topic, i / 2);
      String kafkaValue = value(valueConverter, topic, i, false);
      logger.debug("Sending message with key '{}' and value '{}' to topic '{}'", kafkaKey, kafkaValue, topic);
      connect.kafka().produce(topic, kafkaKey, kafkaValue);
    }

    // wait for tasks to write to BigQuery and commit offsets for their records
    waitForCommittedRecords(CONNECTOR_NAME, topic, NUM_RECORDS_PRODUCED, TASKS_MAX);

    List<List<Object>> allRows = readAllRows(bigQuery, table, KAFKA_FIELD_NAME + ".k1");
    List<List<Object>> expectedRows = LongStream.range(0, NUM_RECORDS_PRODUCED / 2)
        .mapToObj(i -> Arrays.asList(
            "another string",
            (i - 1) % 3 == 0,
            (i * 2 + 1) / 0.69,
            Collections.singletonList(i)))
        .collect(Collectors.toList());
    assertEquals(expectedRows, allRows);
  }

  @Test
  public void testDelete() throws Throwable {
    // create topic in Kafka
    final String topic = "test-delete";
    // Make sure each task gets to read from at least one partition
    connect.kafka().createTopic(topic, TASKS_MAX);

    final String table = "test_delete";
    clearPriorTable(bigQuery, table);

    // setup props for the sink connector
    Map<String, String> props = baseConnectorProps(TASKS_MAX);
    props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);

    props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    props.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");

    // Enable only delete and not upsert, and merge flush every other record
    props.putAll(upsertDeleteProps(false, true, 2));

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // Instantiate the converters we'll use to send records to the connector
    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // Send records to Kafka
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      // Each pair of records will share a key. Because upsert is not enabled, no deduplication will take place
      // and, unless a tombstone is written for that key, both will be inserted
      String kafkaKey = key(keyConverter, topic, i / 2);
      // Every fourth record will be a tombstone, so every record pair with an odd-numbered key will be dropped
      String kafkaValue = value(valueConverter, topic, i, i % 4 == 3);
      logger.debug("Sending message with key '{}' and value '{}' to topic '{}'", kafkaKey, kafkaValue, topic);
      connect.kafka().produce(topic, kafkaKey, kafkaValue);
    }

    // wait for tasks to write to BigQuery and commit offsets for their records
    waitForCommittedRecords(CONNECTOR_NAME, topic, NUM_RECORDS_PRODUCED, TASKS_MAX);

    // Since we have multiple rows per key, order by key and the f3 field (which should be
    // monotonically increasing in insertion order)
    List<List<Object>> allRows = readAllRows(bigQuery, table, KAFKA_FIELD_NAME + ".k1, f3");
    List<List<Object>> expectedRows = LongStream.range(0, NUM_RECORDS_PRODUCED)
        .filter(i -> i % 4 < 2)
        .mapToObj(i -> Arrays.asList(
            i % 4 == 0 ? "a string" : "another string",
            i % 3 == 0,
            i / 0.69,
            Collections.singletonList(i * 2 / 4)))
        .collect(Collectors.toList());
    assertEquals(expectedRows, allRows);
  }

  @Test
  public void testUpsertDelete() throws Throwable {
    // create topic in Kafka
    final String topic = "test-upsert-delete";
    // Make sure each task gets to read from at least one partition
    connect.kafka().createTopic(topic, TASKS_MAX);

    final String table = "test_upsert_delete";
    clearPriorTable(bigQuery, table);

    // setup props for the sink connector
    Map<String, String> props = baseConnectorProps(TASKS_MAX);
    props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);

    props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    props.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");

    // Enable upsert and delete, and merge flush every other record
    props.putAll(upsertDeleteProps(true, true, 2));

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // Instantiate the converters we'll use to send records to the connector
    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // Send records to Kafka
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      // Each pair of records will share a key. Only the second record of each pair should be
      // present in the table at the end of the test
      String kafkaKey = key(keyConverter, topic, i / 2);
      // Every fourth record will be a tombstone, so every record pair with an odd-numbered key will be dropped
      String kafkaValue = value(valueConverter, topic, i, i % 4 == 3);
      logger.debug("Sending message with key '{}' and value '{}' to topic '{}'", kafkaKey, kafkaValue, topic);
      connect.kafka().produce(topic, kafkaKey, kafkaValue);
    }

    // wait for tasks to write to BigQuery and commit offsets for their records
    waitForCommittedRecords(CONNECTOR_NAME, topic, NUM_RECORDS_PRODUCED, TASKS_MAX);

    // Since we have multiple rows per key, order by key and the f3 field (which should be
    // monotonically increasing in insertion order)
    List<List<Object>> allRows = readAllRows(bigQuery, table, KAFKA_FIELD_NAME + ".k1, f3");
    List<List<Object>> expectedRows = LongStream.range(0, NUM_RECORDS_PRODUCED)
        .filter(i -> i % 4 == 1)
        .mapToObj(i -> Arrays.asList(
            "another string",
            i % 3 == 0,
            i / 0.69,
            Collections.singletonList(i * 2 / 4)))
        .collect(Collectors.toList());
    assertEquals(expectedRows, allRows);
  }

  @Test
  @Ignore("Skipped during regular testing; comment-out annotation to run")
  public void testUpsertDeleteHighThroughput() throws Throwable {
    final long numRecords = 1_000_000L;
    final int numPartitions = 10;
    final int tasksMax = 1;

    // create topic in Kafka
    final String topic = "test-upsert-delete-throughput";
    connect.kafka().createTopic(topic, numPartitions);

    final String table = "test_upsert_delete_throughput";
    clearPriorTable(bigQuery, table);

    // Instantiate the converters we'll use to send records to the connector
    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // Send records to Kafka. Pre-populate Kafka before starting the connector as we want to measure
    // the connector's throughput cleanly
    logger.info("Pre-populating Kafka with test data");
    for (int i = 0; i < numRecords; i++) {
      if (i % 10000 == 0) {
        logger.info("{} records produced so far", i);
      }
      // Each pair of records will share a key. Only the second record of each pair should be
      // present in the table at the end of the test
      String kafkaKey = key(keyConverter, topic, i / 2);
      // Every fourth record will be a tombstone, so every record pair with an odd-numbered key will
      // be dropped
      String kafkaValue = value(valueConverter, topic, i, i % 4 == 3);
      connect.kafka().produce(topic, kafkaKey, kafkaValue);
    }

    // setup props for the sink connector
    // use a single task
    Map<String, String> props = baseConnectorProps(tasksMax);
    props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);
    // Allow for at most 10,000 records per call to poll
    props.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX
        + ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
        "10000");
    // Try to get at least 1 MB per partition with each request
    props.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX
        + ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
        Integer.toString(ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES * numPartitions));
    // Wait up to one second for each batch to reach the requested size
    props.put(ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX
        + ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
        "1000"
    );

    props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    props.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");

    // Enable upsert and delete, and schedule ten total flushes
    props.putAll(upsertDeleteProps(true, true, numRecords / 10));

    logger.info("Pre-population complete; creating connector");
    long start = System.currentTimeMillis();
    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, tasksMax);

    // wait for tasks to write to BigQuery and commit offsets for their records
    waitForCommittedRecords(
        CONNECTOR_NAME, Collections.singleton(topic), numRecords, tasksMax, TimeUnit.MINUTES.toMillis(10));
    long time = System.currentTimeMillis() - start;
    logger.info("All records have been read and committed by the connector; "
        + "total time from start to finish: {} seconds", time / 1000.0);

    // Since we have multiple rows per key, order by key and the f3 field (which should be
    // monotonically increasing in insertion order)
    List<List<Object>> allRows = readAllRows(bigQuery, table, KAFKA_FIELD_NAME + ".k1, f3");
    List<List<Object>> expectedRows = LongStream.range(0, numRecords)
        .filter(i -> i % 4 == 1)
        .mapToObj(i -> Arrays.asList(
            "another string",
            i % 3 == 0,
            i / 0.69,
            Collections.singletonList(i * 2 / 4)))
        .collect(Collectors.toList());
    assertEquals(expectedRows, allRows);
  }

  private Converter converter(boolean isKey) {
    Map<String, Object> props = new HashMap<>();
    props.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
    Converter result = new JsonConverter();
    result.configure(props, isKey);
    return result;
  }

  private String key(Converter converter, String topic, long iteration) {
    final Schema schema = SchemaBuilder.struct()
        .field("k1", Schema.INT64_SCHEMA)
        .build();

    final Struct struct = new Struct(schema)
        .put("k1", iteration);

    return new String(converter.fromConnectData(topic, schema, struct));
  }

  private String value(Converter converter, String topic, long iteration, boolean tombstone) {
    final Schema schema = SchemaBuilder.struct()
        .optional()
        .field("f1", Schema.STRING_SCHEMA)
        .field("f2", Schema.BOOLEAN_SCHEMA)
        .field("f3", Schema.FLOAT64_SCHEMA)
        .build();

    if (tombstone) {
      return new String(converter.fromConnectData(topic, schema, null));
    }

    final Struct struct = new Struct(schema)
        .put("f1", iteration % 2 == 0 ? "a string" : "another string")
        .put("f2", iteration % 3 == 0)
        .put("f3", iteration / 0.69);

    return new String(converter.fromConnectData(topic, schema, struct));
  }
}
