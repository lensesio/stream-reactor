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

import com.google.cloud.bigquery.BigQuery;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.SchemaRegistryTestUtils;
import com.wepay.kafka.connect.bigquery.integration.utils.TableClearer;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import io.confluent.connect.avro.AvroConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class UpsertDeleteBigQuerySinkConnectorWithSRIT extends BaseConnectorIT {

  private static final Logger logger = LoggerFactory.getLogger(UpsertDeleteBigQuerySinkConnectorWithSRIT.class);

  private static final String CONNECTOR_NAME = "kcbq-sink-connector";
  private static final long NUM_RECORDS_PRODUCED = 20;
  private static final int TASKS_MAX = 3;
  private static final String KAFKA_FIELD_NAME = "kafkaKey";

  private BigQuery bigQuery;

  private static SchemaRegistryTestUtils schemaRegistry;

  private static String schemaRegistryUrl;

  private Converter keyConverter;

  private Converter valueConverter;
  private Schema valueSchema;

  private Schema keySchema;
  @Before
  public void setup() throws Exception {
    startConnect();
    bigQuery = newBigQuery();

    schemaRegistry = new SchemaRegistryTestUtils(connect.kafka().bootstrapServers());
    schemaRegistry.start();
    schemaRegistryUrl = schemaRegistry.schemaRegistryUrl();

    valueSchema = SchemaBuilder.struct()
            .optional()
            .field("f1", Schema.STRING_SCHEMA)
            .field("f2", Schema.BOOLEAN_SCHEMA)
            .field("f3", Schema.FLOAT64_SCHEMA)
            .build();

    keySchema = SchemaBuilder.struct()
            .field("k1", Schema.INT64_SCHEMA)
            .build();
  }

  @After
  public void close() throws Exception {
    bigQuery = null;
    stopConnect();
    if (schemaRegistry != null) {
      schemaRegistry.stop();
    }
  }

  private Map<String, String> upsertDeleteProps(
      boolean upsert,
      boolean delete,
      long mergeRecordsThreshold) {
    if (!upsert && !delete) {
      throw new IllegalArgumentException("At least one of upsert or delete must be enabled");
    }

    Map<String, String> result = new HashMap<>();

    // use the Avro converter with schemas enabled
    result.put(KEY_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
    result.put(
            ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG,
            schemaRegistryUrl);
    result.put(VALUE_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
    result.put(
            ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG,
            schemaRegistryUrl);

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
    final String topic = suffixedTableOrTopic("test-upsert");
    // Make sure each task gets to read from at least one partition
    connect.kafka().createTopic(topic, TASKS_MAX);

    final String table = sanitizedTable(topic);
    TableClearer.clearTables(bigQuery, dataset(), table);

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
    initialiseConverters();

    List<List<SchemaAndValue>> records = new ArrayList<>();

    // Prepare records
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      // Each pair of records will share a key. Only the second record of each pair should be
      // present in the table at the end of the test
      List<SchemaAndValue> record = new ArrayList<>();
              SchemaAndValue schemaAndValue = new SchemaAndValue(valueSchema, data(i));
      SchemaAndValue keyschemaAndValue = new SchemaAndValue(keySchema, new Struct(keySchema)
              .put("k1", i/2l));

      logger.debug("Sending message with key '{}' and value '{}' to topic '{}'", keyschemaAndValue, schemaAndValue, topic);

      record.add(keyschemaAndValue);
      record.add(schemaAndValue);

      records.add(record);
    }

    // send prepared records
    schemaRegistry.produceRecordsWithKey(keyConverter, valueConverter, records, topic);

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
    final String topic = suffixedTableOrTopic("test-delete");
    // Make sure each task gets to read from at least one partition
    connect.kafka().createTopic(topic, TASKS_MAX);

    final String table = sanitizedTable(topic);
    TableClearer.clearTables(bigQuery, dataset(), table);

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
    initialiseConverters();

    List<List<SchemaAndValue>> records = new ArrayList<>();

    // Prepare records
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      // Each pair of records will share a key. Because upsert is not enabled, no deduplication will take place
      // and, unless a tombstone is written for that key, both will be inserted
      List<SchemaAndValue> record = new ArrayList<>();
      SchemaAndValue schemaAndValue;
      // Every fourth record will be a tombstone, so every record pair with an odd-numbered key will be dropped
      if(i % 4 == 3) {
        schemaAndValue = new SchemaAndValue(valueSchema,null);
      } else {
        schemaAndValue = new SchemaAndValue(valueSchema, data(i));
      }
      SchemaAndValue keyschemaAndValue = new SchemaAndValue(keySchema, new Struct(keySchema)
              .put("k1", i/ 2L));

      logger.debug("Sending message with key '{}' and value '{}' to topic '{}'", keyschemaAndValue, schemaAndValue, topic);

      record.add(keyschemaAndValue);
      record.add(schemaAndValue);

      records.add(record);
    }

    // send prepared records
    schemaRegistry.produceRecordsWithKey(keyConverter, valueConverter, records, topic);

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
    final String topic = suffixedTableOrTopic("test-upsert-delete");
    // Make sure each task gets to read from at least one partition
    connect.kafka().createTopic(topic, TASKS_MAX);

    final String table = sanitizedTable(topic);
    TableClearer.clearTables(bigQuery, dataset(), table);

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
    initialiseConverters();

    List<List<SchemaAndValue>> records = new ArrayList<>();

    // Prepare records
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      // Each pair of records will share a key. Only the second record of each pair should be
      // present in the table at the end of the test
      List<SchemaAndValue> record = new ArrayList<>();
      SchemaAndValue schemaAndValue;
      // Every fourth record will be a tombstone, so every record pair with an odd-numbered key will be dropped
      if(i % 4 == 3) {
        schemaAndValue = new SchemaAndValue(valueSchema,null);
      } else {
        schemaAndValue = new SchemaAndValue(valueSchema, data(i));
      }

      SchemaAndValue keyschemaAndValue = new SchemaAndValue(keySchema, new Struct(keySchema)
              .put("k1", i/ 2L));

      logger.debug("Sending message with key '{}' and value '{}' to topic '{}'", keyschemaAndValue, schemaAndValue, topic);

      record.add(keyschemaAndValue);
      record.add(schemaAndValue);

      records.add(record);
    }

    // send prepared records
    schemaRegistry.produceRecordsWithKey(keyConverter, valueConverter, records, topic);

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

  private void initialiseConverters() {
    keyConverter = new AvroConverter();
    valueConverter = new AvroConverter();
    keyConverter.configure(Collections.singletonMap(
                    SCHEMA_REGISTRY_URL_CONFIG,schemaRegistryUrl
            ), true
    );
    valueConverter.configure(Collections.singletonMap(
                    SCHEMA_REGISTRY_URL_CONFIG,schemaRegistryUrl
            ), false
    );
  }
  private Struct data(long iteration) {
    return new Struct(valueSchema)
            .put("f1", iteration % 2 == 0 ? "a string" : "another string")
            .put("f2", iteration % 3 == 0)
            .put("f3", iteration / 0.69);
  }

}
