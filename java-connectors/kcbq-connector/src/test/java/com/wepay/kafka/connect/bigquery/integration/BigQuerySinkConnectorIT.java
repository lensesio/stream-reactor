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

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.BucketClearer;
import com.wepay.kafka.connect.bigquery.integration.utils.SchemaRegistryTestUtils;
import com.wepay.kafka.connect.bigquery.integration.utils.TableClearer;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.formatter.AvroMessageReader;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

import static com.wepay.kafka.connect.bigquery.integration.BaseConnectorIT.boxByteArray;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class BigQuerySinkConnectorIT {

  @Parameterized.Parameters
  public static Iterable<Object[]> testCases() {
    Collection<Object[]> result = new ArrayList<>();

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
    result.add(new Object[] {"gcs-load", expectedGcsLoadRows});

    List<List<Object>> expectedNullsRows = new ArrayList<>();
    expectedNullsRows.add(Arrays.asList(1L, "Required string", null, 42L, false));
    expectedNullsRows.add(Arrays.asList(2L, "Required string", "Optional string", 89L, null));
    expectedNullsRows.add(Arrays.asList(3L, "Required string", null, null, true));
    expectedNullsRows.add(Arrays.asList(4L, "Required string", "Optional string", null, null));
    result.add(new Object[] {"nulls", expectedNullsRows});

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
    result.add(new Object[] {"matryoshka-dolls", expectedMatryoshkaRows});

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
    result.add(new Object[] {"primitives", expectedPrimitivesRows});

    List<List<Object>> expectedLogicalTypesRows = new ArrayList<>();
    expectedLogicalTypesRows.add(Arrays.asList(1L, 0L, 0L));
    expectedLogicalTypesRows.add(Arrays.asList(2L, 42000000000L, 362880000000L));
    expectedLogicalTypesRows.add(Arrays.asList(3L, 1468275102000000L, 1468195200000L));
    result.add(new Object[] {"logical-types", expectedLogicalTypesRows});

    return result;
  }

  private static final String TEST_CASE_PREFIX = "kcbq_test_";

  // Share a single embedded Connect and Schema Registry cluster for all test cases to keep the runtime down
  private static BaseConnectorIT testBase;
  private static SchemaRegistryTestUtils schemaRegistry;
  private static String schemaRegistryUrl;

  private final String testCase;
  private final List<List<Object>> expectedRows;
  private final String topic;
  private final String table;
  private final String connectorName;

  private Producer<byte[], byte[]> valueProducer;
  private int numRecordsProduced;

  public BigQuerySinkConnectorIT(String testCase, List<List<Object>> expectedRows) {
    this.testCase = testCase;
    this.expectedRows = expectedRows;

    this.topic = TEST_CASE_PREFIX + testCase;
    this.table = testBase.suffixedAndSanitizedTable(topic);
    this.connectorName = "bigquery-connector-" + testCase;
  }

  @BeforeClass
  public static void globalSetup() throws Exception {
    testBase = new BaseConnectorIT() {};
    testBase.startConnect();

    schemaRegistry = new SchemaRegistryTestUtils(testBase.connect.kafka().bootstrapServers());

    schemaRegistry.start();

    schemaRegistryUrl = schemaRegistry.schemaRegistryUrl();

    BucketClearer.clearBucket(
      testBase.keyFile(),
      testBase.project(),
      testBase.gcsBucket(),
      testBase.gcsFolder(),
      testBase.keySource()
    );
  }

  @Before
  public void setup() {
    TableClearer.clearTables(testBase.newBigQuery(), testBase.dataset(), table);

    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testBase.connect.kafka().bootstrapServers());
    valueProducer = new KafkaProducer<>(
        producerProps, Serdes.ByteArray().serializer(), Serdes.ByteArray().serializer());

    numRecordsProduced = 0;
  }

  @After
  public void cleanup() {
    testBase.connect.deleteConnector(connectorName);
  }

  @AfterClass
  public static void globalCleanup() throws Exception {
    if (schemaRegistry != null) {
     schemaRegistry.stop();
    }
    testBase.stopConnect();
  }

  @Test
  public void runTestCase() throws Exception {
    final int tasksMax = 1;

    populate();

    testBase.connect.configureConnector(connectorName, connectorProps(tasksMax));

    testBase.waitForConnectorToStart(connectorName, tasksMax);

    testBase.waitForCommittedRecords(
        connectorName, Collections.singleton(topic), numRecordsProduced, tasksMax, TimeUnit.MINUTES.toMillis(3));

    verify();
  }

  private void populate() {
    testBase.connect.kafka().createTopic(topic);

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
    Map<String, String> result = testBase.baseConnectorProps(tasksMax);

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

    result.put(SinkConnectorConfig.TOPICS_CONFIG, topic);
    
    result.put(BigQuerySinkConfig.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG, "true");
    result.put(BigQuerySinkConfig.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG, "true");
    result.put(BigQuerySinkConfig.ENABLE_BATCH_CONFIG, testBase.suffixedAndSanitizedTable("kcbq_test_gcs-load"));
    result.put(BigQuerySinkConfig.BATCH_LOAD_INTERVAL_SEC_CONFIG, "10");
    result.put(BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG, testBase.gcsBucket() + System.nanoTime());
    result.put(BigQuerySinkConfig.GCS_FOLDER_NAME_CONFIG, testBase.gcsFolder());
    result.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());

    String suffix = testBase.tableSuffix();
    if (!suffix.isEmpty()) {
      String escapedSuffix = suffix.replaceAll("\\\\", "\\\\\\\\").replaceAll("\\$", "\\\\\\$");
      result.put("transforms", "addSuffix");
      result.put("transforms.addSuffix.type", "org.apache.kafka.connect.transforms.RegexRouter");
      result.put("transforms.addSuffix.regex", "(.*)");
      result.put("transforms.addSuffix.replacement", "$1" + escapedSuffix);
    }

    return result;
  }

  private void verify() {
    List<List<Object>> testRows;
    try {
      String table = testBase.suffixedAndSanitizedTable(TEST_CASE_PREFIX + FieldNameSanitizer.sanitizeName(testCase));
      testRows = testBase.readAllRows(testBase.newBigQuery(), table, "row");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    assertEquals(expectedRows, testRows);
  }
}
