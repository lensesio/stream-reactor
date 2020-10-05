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

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.BucketClearer;
import com.wepay.kafka.connect.bigquery.integration.utils.TableClearer;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.formatter.AvroMessageReader;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import kafka.common.MessageReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class BigQuerySinkConnectorIT extends BaseConnectorIT {

  private static final Map<String, List<List<Object>>> TEST_CASES;
  static {
    Map<String, List<List<Object>>> testCases = new HashMap<>();

    List<List<Object>> expectedGcsLoadRows = new ArrayList<>();
    expectedGcsLoadRows.add(Arrays.asList(
        1L,
        null,
        false,
        4242L,
        42424242424242L,
        42.42,
        42424242.42424242,
        "forty-two",
        boxByteArray(new byte[] { 0x0, 0xf, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78 })
    ));
    expectedGcsLoadRows.add(Arrays.asList(
        2L,
        5L,
        true,
        4354L,
        435443544354L,
        43.54,
        435443.544354,
        "forty-three",
        boxByteArray(new byte[] { 0x0, 0xf, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78 })
    ));
    expectedGcsLoadRows.add(Arrays.asList(
        3L,
        8L,
        false,
        1993L,
        199319931993L,
        19.93,
        199319.931993,
        "nineteen",
        boxByteArray(new byte[] { 0x0, 0xf, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78 })
    ));
    testCases.put("gcs-load", expectedGcsLoadRows);

    List<List<Object>> expectedNullsRows = new ArrayList<>();
    expectedNullsRows.add(Arrays.asList(1L, "Required string", null, 42L, false));
    expectedNullsRows.add(Arrays.asList(2L, "Required string", "Optional string", 89L, null));
    expectedNullsRows.add(Arrays.asList(3L, "Required string", null, null, true));
    expectedNullsRows.add(Arrays.asList(4L, "Required string", "Optional string", null, null));
    testCases.put("nulls", expectedNullsRows);

    List<List<Object>> expectedMatryoshkaRows = new ArrayList<>();
    expectedMatryoshkaRows.add(Arrays.asList(
        1L,
        Arrays.asList(
            Arrays.asList(42.0, 42.42, 42.4242),
            Arrays.asList(
                42L,
                "42"
            )
        ),
        Arrays.asList(
            -42L,
            "-42"
        )
    ));
    testCases.put("matryoshka-dolls", expectedMatryoshkaRows);

    List<List<Object>> expectedPrimitivesRows = new ArrayList<>();
    expectedPrimitivesRows.add(Arrays.asList(
        1L,
        null,
        false,
        4242L,
        42424242424242L,
        42.42,
        42424242.42424242,
        "forty-two",
        boxByteArray(new byte[] { 0x0, 0xf, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78 })
    ));
    testCases.put("primitives", expectedPrimitivesRows);

    List<List<Object>> expectedLogicalTypesRows = new ArrayList<>();
    expectedLogicalTypesRows.add(Arrays.asList(1L, 0L, 0L));
    expectedLogicalTypesRows.add(Arrays.asList(2L, 42000000000L, 362880000000L));
    expectedLogicalTypesRows.add(Arrays.asList(3L, 1468275102000000L, 1468195200000L));
    testCases.put("logical-types", expectedLogicalTypesRows);

    TEST_CASES = Collections.unmodifiableMap(testCases);
  }

  private static final String TEST_CASE_PREFIX = "kcbq_test_";

  private static final Collection<String> TEST_TOPICS = TEST_CASES.keySet().stream()
      .map(tc -> TEST_CASE_PREFIX + tc)
      .collect(Collectors.toList());

  private static final Collection<String> TEST_TABLES = TEST_TOPICS.stream()
      .map(FieldNameSanitizer::sanitizeName)
      .collect(Collectors.toList());

  private RestApp restApp;
  private String schemaRegistryUrl;
  private Producer<byte[], byte[]> valueProducer;
  private int numRecordsProduced;

  @Before
  public void setup() throws Exception {
    BucketClearer.clearBucket(keyFile(), project(), gcsBucket(), keySource());
    TableClearer.clearTables(newBigQuery(), dataset(), TEST_TABLES);

    startConnect();
    restApp = new RestApp(
        ClusterTestHarness.choosePort(),
        null,
        connect.kafka().bootstrapServers(),
        SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC,
        CompatibilityLevel.BACKWARD.name,
        true,
        null);

    restApp.start();

    schemaRegistryUrl = restApp.restClient.getBaseUrls().current();

    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connect.kafka().bootstrapServers());
    valueProducer = new KafkaProducer<>(
        producerProps, Serdes.ByteArray().serializer(), Serdes.ByteArray().serializer());

    numRecordsProduced = 0;
  }

  @After
  public void close() throws Exception {
    if (restApp != null) {
      restApp.stop();
    }
    stopConnect();
  }

  @Test
  public void testAll() throws Exception {
    final int tasksMax = 1;
    final String connectorName = "bigquery-connector";

    TEST_CASES.keySet().forEach(this::populate);

    connect.configureConnector(connectorName, connectorProps(tasksMax));

    waitForConnectorToStart(connectorName, tasksMax);

    waitForCommittedRecords(
        "bigquery-connector", TEST_TOPICS, numRecordsProduced, tasksMax, TimeUnit.MINUTES.toMillis(3));

    TEST_CASES.forEach(this::verify);
  }

  private void populate(String testCase) {
    String topic = TEST_CASE_PREFIX + testCase;
    connect.kafka().createTopic(topic);

    String testCaseDir = "integration_test_cases/" + testCase + "/";

    InputStream schemaStream = BigQuerySinkConnectorIT.class.getClassLoader()
        .getResourceAsStream(testCaseDir + "schema.json");
    Scanner schemaScanner = new Scanner(schemaStream).useDelimiter("\\A");
    String schemaString = schemaScanner.next();

    Properties messageReaderProps = new Properties();
    messageReaderProps.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    messageReaderProps.put("value.schema", schemaString);
    messageReaderProps.put("topic", topic);
    InputStream dataStream = BigQuerySinkConnectorIT.class.getClassLoader()
        .getResourceAsStream(testCaseDir + "data.json");
    MessageReader messageReader = new AvroMessageReader();
    messageReader.init(dataStream, messageReaderProps);

    ProducerRecord<byte[], byte[]> message = messageReader.readMessage();
    while (message != null) {
      try {
        valueProducer.send(message).get(1, TimeUnit.SECONDS);
        numRecordsProduced++;
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw new RuntimeException(e);
      }
      message = messageReader.readMessage();
    } 
  }

  private Map<String, String> connectorProps(int tasksMax) {
    Map<String, String> result = baseConnectorProps(tasksMax);

    result.put(
        ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG,
        AvroConverter.class.getName());
    result.put(
        ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistryUrl);
    result.put(
        ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG,
        AvroConverter.class.getName());
    result.put(
        ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistryUrl);

    result.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", TEST_TOPICS));
    
    result.put(BigQuerySinkConfig.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG, "true");
    result.put(BigQuerySinkConfig.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG, "true");
    result.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    result.put(BigQuerySinkConfig.ENABLE_BATCH_CONFIG, "kcbq_test_gcs-load");
    result.put(BigQuerySinkConfig.BATCH_LOAD_INTERVAL_SEC_CONFIG, "10");
    result.put(BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG, gcsBucket());
    result.put(BigQuerySinkConfig.GCS_FOLDER_NAME_CONFIG, gcsFolder());
    result.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());

    return result;
  }

  private void verify(String testCase, List<List<Object>> expectedRows) {
    List<List<Object>> testRows;
    try {
      testRows = readAllRows(newBigQuery(), TEST_CASE_PREFIX + FieldNameSanitizer.sanitizeName(testCase), "row");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    assertEquals(expectedRows, testRows);
  }
}
