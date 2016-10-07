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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import static org.mockito.Matchers.any;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.SinkConfigConnectException;

import com.wepay.kafka.connect.bigquery.write.batch.SingleBatchWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.errors.ConnectException;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BigQuerySinkTaskTest {
  private static SinkTaskPropertiesFactory propertiesFactory;

  @BeforeClass
  public static void initializePropertiesFactory() {
    propertiesFactory = new SinkTaskPropertiesFactory();
  }

  @Test
  public void testSimplePut() {
    final String topic = "test-topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkTaskConfig.BUFFER_SIZE_CONFIG, "-1");
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, ".*=scratch");

    BigQuery bigQuery = mock(BigQuery.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery);
    testTask.start(properties);

    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    verify(bigQuery, never()).insertAll(any(InsertAllRequest.class));
  }

  @Test
  public void testEmptyPut() {
    Map<String, String> properties = propertiesFactory.getProperties();
    BigQuery bigQuery = mock(BigQuery.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery);
    testTask.start(properties);

    testTask.put(Collections.emptyList());
  }

  @Test
  public void testEmptyRecordPut() {
    final String topic = "test_topic";
    final Schema simpleSchema = SchemaBuilder
        .struct()
        .field("aField", Schema.STRING_SCHEMA)
        .build();

    Map<String, String> properties = propertiesFactory.getProperties();
    BigQuery bigQuery = mock(BigQuery.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery);
    testTask.start(properties);

    SinkRecord emptyRecord = spoofSinkRecord(topic, simpleSchema, null);

    testTask.put(Collections.singletonList(emptyRecord));
  }

  // It's important that the buffer be completely wiped after a call to flush, since any execption
  // thrown during flush causes Kafka Connect to not commit the offsets for any records sent to the
  // task since the last flush
  @Test
  public void testBufferClearOnFlushError() {
    final String dataset = "scratch";
    final String topic = "test_topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkTaskConfig.BUFFER_SIZE_CONFIG, "-1");
    properties.put(BigQuerySinkTaskConfig.BATCH_WRITER_CONFIG,
                   SingleBatchWriter.class.getCanonicalName());
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, String.format(".*=%s", dataset));

    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.insertAll(any(InsertAllRequest.class)))
        .thenThrow(new RuntimeException("This is a test"));

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    when(sinkTaskContext.assignment()).thenReturn(Collections.emptySet());
    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);

    try {
      testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
      testTask.flush(Collections.emptyMap());
      fail("An exception should have been thrown by now");
    } catch (BigQueryConnectException err) {
      testTask.flush(Collections.emptyMap());
      verify(bigQuery, times(1)).insertAll(any(InsertAllRequest.class));
    }
  }

  @Test
  public void testEmptyFlush() {
    Map<String, String> properties = propertiesFactory.getProperties();
    BigQuery bigQuery = mock(BigQuery.class);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    when(sinkTaskContext.assignment()).thenReturn(Collections.emptySet());
    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);

    testTask.flush(null);
  }

  @Test
  public void testBigQuery5XXRetry() {
    final String topic = "test_topic";
    final String dataset = "scratch";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_RETRY_CONFIG, "1");
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_RETRY_WAIT_CONFIG, "2000");
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, String.format(".*=%s", dataset));

    BigQuery bigQuery = mock(BigQuery.class);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    when(bigQuery.insertAll(anyObject()))
        .thenThrow(new BigQueryException(500, "mock 500"))
        .thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    when(sinkTaskContext.assignment()).thenReturn(Collections.emptySet());

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    testTask.flush(Collections.emptyMap());

    verify(bigQuery, times(2)).insertAll(anyObject());
  }

  @Test
  public void testBigQuery403Retry() {
    final String topic = "test_topic";
    final String dataset = "scratch";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_RETRY_CONFIG, "1");
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_RETRY_WAIT_CONFIG, "2000");
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, String.format(".*=%s", dataset));

    BigQuery bigQuery = mock(BigQuery.class);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    BigQueryError quotaExceededError = new BigQueryError("quotaExceeded", null, null);
    when(bigQuery.insertAll(anyObject()))
        .thenThrow(new BigQueryException(403, "mock quota exceeded", quotaExceededError))
        .thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    when(sinkTaskContext.assignment()).thenReturn(Collections.emptySet());

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    testTask.flush(Collections.emptyMap());

    verify(bigQuery, times(2)).insertAll(anyObject());
  }

  @Test(expected = BigQueryConnectException.class)
  public void testBigQueryRetryExceeded() {
    final String topic = "test_topic";
    final String dataset = "scratch";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_RETRY_CONFIG, "1");
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_RETRY_WAIT_CONFIG, "2000");
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, String.format(".*=%s", dataset));

    BigQuery bigQuery = mock(BigQuery.class);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    BigQueryError quotaExceededError = new BigQueryError("quotaExceeded", null, null);
    when(bigQuery.insertAll(anyObject()))
      .thenThrow(new BigQueryException(403, "mock quota exceeded", quotaExceededError));
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    when(sinkTaskContext.assignment()).thenReturn(Collections.emptySet());

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    testTask.flush(Collections.emptyMap());
  }

  // Make sure that an InterruptedException is properly translated into a ConnectException
  @Test(expected = ConnectException.class)
  public void testInterruptedException() {
    final String dataset = "scratch";
    final String topic = "test_topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkTaskConfig.BUFFER_SIZE_CONFIG, "-1");
    properties.put(BigQuerySinkTaskConfig.BATCH_WRITER_CONFIG,
                   SingleBatchWriter.class.getCanonicalName());
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, String.format(".*=%s", dataset));

    BigQuery bigQuery  = mock(BigQuery.class);
    InsertAllResponse fakeResponse = mock(InsertAllResponse.class);
    when(fakeResponse.hasErrors()).thenReturn(false);
    when(fakeResponse.insertErrors()).thenReturn(Collections.emptyMap());
    when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(fakeResponse);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    when(sinkTaskContext.assignment()).thenReturn(Collections.emptySet());
    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);

    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    testTask.flush(Collections.emptyMap());

    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    Thread.currentThread().interrupt();
    testTask.flush(Collections.emptyMap());
  }

  // Make sure that a ConfigException is properly translated into a SinkConfigConnectException
  @Test(expected = SinkConfigConnectException.class)
  public void testConfigException() {
    Map<String, String> badProperties = propertiesFactory.getProperties();
    badProperties.remove(BigQuerySinkConfig.TOPICS_CONFIG);

    BigQuerySinkTask testTask = new BigQuerySinkTask(mock(BigQuery.class));
    testTask.start(badProperties);
  }

  @Test
  public void testTablesToTopics() {
    BigQuery bigQuery  = mock(BigQuery.class);
    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery);
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "sanitize-me,leave_me_alone"
    );
    testTask.start(configProperties);

    Map<TableId, String> expectedTablesToSchemas = new HashMap<>();
    expectedTablesToSchemas.put(TableId.of("scratch", "sanitize_me"), "sanitize-me");
    expectedTablesToSchemas.put(TableId.of("scratch", "leave_me_alone"), "leave_me_alone");

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    Map<String, String> topicsToDatasets = testConfig.getTopicsToDatasets();
    Map<TableId, String> testTablesToSchemas = testTask.getTablesToTopics(topicsToDatasets);

    assertEquals(expectedTablesToSchemas, testTablesToSchemas);
  }

  @Test
  public void testVersion() {
    assertNotNull(new BigQuerySinkTask().version());
  }

  // Doesn't do anything at the moment, but having this here will encourage tests to be written if
  // the stop() method ever does anything significant
  @Test
  public void testStop() {
    new BigQuerySinkTask().stop();
  }

  /**
   * Utility method for spoofing InsertAllRequests that should be sent to a BigQuery object.
   * @param table The table to write to.
   * @param rows The rows to write.
   * @return The spoofed InsertAllRequest.
   */
  public static InsertAllRequest buildExpectedInsertAllRequest(
      TableId table,
      InsertAllRequest.RowToInsert... rows) {
    return InsertAllRequest.builder(table, rows)
        .ignoreUnknownValues(false)
        .skipInvalidRows(false)
        .build();
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put()
   * @param topic The topic of the record.
   * @param valueSchema The schema of the record.
   * @param value The content of the record.
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic, Schema valueSchema, Struct value) {
    return new SinkRecord(topic, 0, null, null, valueSchema, value, 0);
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put(). Creates a
   * record with a struct schema that contains only one string field with a given name and value.
   * @param topic The topic of the record.
   * @param field The name of the field in the record's struct.
   * @param value The content of the field.
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic, String field, String value) {
    Schema basicRowSchema = SchemaBuilder
        .struct()
        .field(field, Schema.STRING_SCHEMA)
        .build();
    Struct basicRowValue = new Struct(basicRowSchema);
    basicRowValue.put(field, value);
    return spoofSinkRecord(topic, basicRowSchema, basicRowValue);
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put(). Creates a
   * record with a struct schema that contains only one string field with a default name and a given
   * value.
   * @param topic The topic of the record.
   * @param value The content of the record.
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic, String value) {
    return spoofSinkRecord(topic, "sink_task_test_field", value);
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put(). Creates a
   * record with a struct schema that contains only one string field with a default name and a
   * default value.
   * @param topic The topic of the record.
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic) {
    return spoofSinkRecord(topic, "sink task test row");
  }
}
