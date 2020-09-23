package com.wepay.kafka.connect.bigquery.write.row;

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
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.storage.Storage;

import com.wepay.kafka.connect.bigquery.BigQuerySinkTask;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.SinkTaskPropertiesFactory;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import org.junit.BeforeClass;
import org.junit.Test;

import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
public class BigQueryWriterTest {
  private static SinkTaskPropertiesFactory propertiesFactory;

  @BeforeClass
  public static void initializePropertiesFactory() {
    propertiesFactory = new SinkTaskPropertiesFactory();
  }

  @Test
  public void testBigQueryNoFailure() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);

    BigQuery bigQuery = mock(BigQuery.class);
    Map<Long, List<BigQueryError>> emptyMap = mock(Map.class);
    when(emptyMap.isEmpty()).thenReturn(true);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    when(insertAllResponse.hasErrors()).thenReturn(false);
    when(insertAllResponse.getInsertErrors()).thenReturn(emptyMap);

    //first attempt (success)
    when(bigQuery.insertAll(anyObject()))
            .thenReturn(insertAllResponse);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    Storage storage = mock(Storage.class);
    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    verify(bigQuery, times(1)).insertAll(anyObject());
  }

  @Test
  public void testAutoCreateTables() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);
    properties.put(BigQuerySinkTaskConfig.TABLE_CREATE_CONFIG, "true");

    BigQuery bigQuery = mock(BigQuery.class);
    Map<Long, List<BigQueryError>> emptyMap = mock(Map.class);
    when(emptyMap.isEmpty()).thenReturn(true);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    when(insertAllResponse.hasErrors()).thenReturn(false);
    when(insertAllResponse.getInsertErrors()).thenReturn(emptyMap);

    BigQueryException missTableException = new BigQueryException(404, "Table is missing");

    when(bigQuery.insertAll(anyObject())).thenThrow(missTableException).thenReturn(insertAllResponse);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    Storage storage = mock(Storage.class);
    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
            Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    verify(schemaManager, times(1)).createTable(anyObject(), anyObject());
    verify(bigQuery, times(2)).insertAll(anyObject());
  }

  @Test
  public void testNonAutoCreateTables() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);

    BigQuery bigQuery = mock(BigQuery.class);

    Map<Long, List<BigQueryError>> emptyMap = mock(Map.class);
    when(emptyMap.isEmpty()).thenReturn(true);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    when(insertAllResponse.hasErrors()).thenReturn(false);
    when(insertAllResponse.getInsertErrors()).thenReturn(emptyMap);

    BigQueryException missTableException = new BigQueryException(404, "Table is missing");

    when(bigQuery.insertAll(anyObject())).thenThrow(missTableException).thenReturn(insertAllResponse);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    Storage storage = mock(Storage.class);
    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
            Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    verify(schemaManager, times(0)).createTable(anyObject(), anyObject());
    verify(bigQuery, times(2)).insertAll(anyObject());
  }

  @Test
  public void testBigQueryPartialFailure() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);
    final Set<Long> failedRowSet = new HashSet<>();
    failedRowSet.add(1L);

    Map<Long, List<BigQueryError>> insertErrorMap = mock(Map.class);
    when(insertErrorMap.isEmpty()).thenReturn(false);
    when(insertErrorMap.size()).thenReturn(1);
    when(insertErrorMap.keySet()).thenReturn(failedRowSet);

    InsertAllResponse insertAllResponseWithError = mock(InsertAllResponse.class);
    when(insertAllResponseWithError.hasErrors()).thenReturn(true);
    when(insertAllResponseWithError.getInsertErrors()).thenReturn(insertErrorMap);

    Map<Long, List<BigQueryError>> emptyMap = mock(Map.class);
    when(emptyMap.isEmpty()).thenReturn(true);

    InsertAllResponse insertAllResponseNoError = mock(InsertAllResponse.class);
    when(insertAllResponseNoError.hasErrors()).thenReturn(true);
    when(insertAllResponseNoError.getInsertErrors()).thenReturn(emptyMap);

    BigQuery bigQuery = mock(BigQuery.class);

    //first attempt (partial failure); second attempt (success)
    when(bigQuery.insertAll(anyObject()))
        .thenReturn(insertAllResponseWithError)
        .thenReturn(insertAllResponseNoError);

    List<SinkRecord> sinkRecordList = new ArrayList<>();
    sinkRecordList.add(spoofSinkRecord(topic, 0, 0, "some_field", "some_value"));
    sinkRecordList.add(spoofSinkRecord(topic, 1, 1, "some_field", "some_value"));

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    Storage storage = mock(Storage.class);
    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(sinkRecordList);
    testTask.flush(Collections.emptyMap());

    ArgumentCaptor<InsertAllRequest> varArgs = ArgumentCaptor.forClass(InsertAllRequest.class);
    verify(bigQuery, times(2)).insertAll(varArgs.capture());

    assertEquals(2, varArgs.getAllValues().get(0).getRows().size());
    //second insertAll is called with just the failed rows
    assertEquals(1, varArgs.getAllValues().get(1).getRows().size());
    assertEquals("test_topic-1-1", varArgs.getAllValues().get(1).getRows().get(0).getId());
  }

  @Test(expected = BigQueryConnectException.class)
  public void testBigQueryCompleteFailure() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);

    Map<Long, List<BigQueryError>> insertErrorMap = mock(Map.class);
    when(insertErrorMap.isEmpty()).thenReturn(false);
    when(insertErrorMap.size()).thenReturn(2);

    InsertAllResponse insertAllResponseWithError = mock(InsertAllResponse.class);
    when(insertAllResponseWithError.hasErrors()).thenReturn(true);
    when(insertAllResponseWithError.getInsertErrors()).thenReturn(insertErrorMap);

    Map<Long, List<BigQueryError>> emptyMap = mock(Map.class);
    when(emptyMap.isEmpty()).thenReturn(true);

    InsertAllResponse insertAllResponseNoError = mock(InsertAllResponse.class);
    when(insertAllResponseNoError.hasErrors()).thenReturn(true);
    when(insertAllResponseNoError.getInsertErrors()).thenReturn(emptyMap);

    BigQuery bigQuery = mock(BigQuery.class);

    //first attempt (complete failure); second attempt (not expected)
    when(bigQuery.insertAll(anyObject()))
        .thenReturn(insertAllResponseWithError);

    List<SinkRecord> sinkRecordList = new ArrayList<>();
    sinkRecordList.add(spoofSinkRecord(topic, 0, 0, "some_field", "some_value"));
    sinkRecordList.add(spoofSinkRecord(topic, 1, 1, "some_field", "some_value"));

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    Storage storage = mock(Storage.class);
    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(sinkRecordList);
    testTask.flush(Collections.emptyMap());
  }

  /**
   * Utility method for making and retrieving properties based on provided parameters.
   * @param bigqueryRetry The number of retries.
   * @param bigqueryRetryWait The wait time for each retry.
   * @param topic The topic of the record.
   * @param dataset The dataset of the record.
   * @return The map of bigquery sink configurations.
   */
  private Map<String,String> makeProperties(String bigqueryRetry,
                                            String bigqueryRetryWait,
                                            String topic,
                                            String dataset) {
    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_RETRY_CONFIG, bigqueryRetry);
    properties.put(BigQuerySinkTaskConfig.BIGQUERY_RETRY_WAIT_CONFIG, bigqueryRetryWait);
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DATASETS_CONFIG, String.format(".*=%s", dataset));
    return properties;
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put()
   * @param topic The topic of the record.
   * @param partition The partition of the record.
   * @param field The name of the field in the record's struct.
   * @param value The content of the field.
   * @return The spoofed SinkRecord.
   */
  private SinkRecord spoofSinkRecord(String topic,
                                     int partition,
                                     long kafkaOffset,
                                     String field,
                                     String value) {
    Schema basicRowSchema = SchemaBuilder
            .struct()
            .field(field, Schema.STRING_SCHEMA)
            .build();
    Struct basicRowValue = new Struct(basicRowSchema);
    basicRowValue.put(field, value);
    return new SinkRecord(topic,
                          partition,
                          null,
                          null,
                          basicRowSchema,
                          basicRowValue,
                          kafkaOffset,
                          null,
                          null);
  }
}
