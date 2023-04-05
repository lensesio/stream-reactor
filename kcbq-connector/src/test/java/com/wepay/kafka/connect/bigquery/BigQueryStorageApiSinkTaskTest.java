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

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;

import com.google.cloud.storage.Storage;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.write.storageApi.StorageWriteApiDefaultStream;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.json.JSONArray;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.mockito.ArgumentMatchers;


import java.net.SocketTimeoutException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.CONNECTOR_RUNTIME_PROVIDER_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.CONNECTOR_RUNTIME_PROVIDER_DEFAULT;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;


public class BigQueryStorageApiSinkTaskTest {
    private static SinkTaskPropertiesFactory propertiesFactory;

    private static AtomicLong spoofedRecordOffset = new AtomicLong();
    private static StorageWriteApiDefaultStream mockedStorageWriteApiDefaultStream;

    @BeforeClass
    public static void initializePropertiesFactory() {
        propertiesFactory = new SinkTaskPropertiesFactory();
    }

    @Before
    public void setUp() {
        spoofedRecordOffset.set(0);
        mockedStorageWriteApiDefaultStream = mock(
                StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
    }

    @Test
    public void testSimplePut() throws Exception {
        final String topic = "test-topic";

        Map<String, String> properties = propertiesFactory.getProperties();
        properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
        properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
        properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");

        BigQuery bigQuery = mock(BigQuery.class);

        Storage storage = mock(Storage.class);

        SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
                .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType()).build();

        doReturn(mockedStreamWriter).when(mockedStorageWriteApiDefaultStream).getDefaultStream(any(), any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenReturn(successResponse);

        SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        Map<TableId, Table> cache = new HashMap<>();

        BigQuerySinkTask testTask = new BigQuerySinkTask(
                bigQuery, schemaRetriever, storage, schemaManager, cache, mockedStorageWriteApiDefaultStream);
        testTask.initialize(sinkTaskContext);
        testTask.start(properties);

        testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
        testTask.flush(Collections.emptyMap());
        verify(mockedStreamWriter, times(1)).append(any(JSONArray.class));
    }

    @Test
    public void testSimplePutWhenSchemaRetrieverIsNotNull() throws Exception {
        final String topic = "test-topic";

        Map<String, String> properties = propertiesFactory.getProperties();
        properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
        properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
        properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");

        BigQuery bigQuery = mock(BigQuery.class);

        Storage storage = mock(Storage.class);
        SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
                .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType()).build();

        doReturn(mockedStreamWriter).when(mockedStorageWriteApiDefaultStream).getDefaultStream(any(), any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenReturn(successResponse);

        SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        Map<TableId, Table> cache = new HashMap<>();

        BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager, cache, mockedStorageWriteApiDefaultStream);
        testTask.initialize(sinkTaskContext);
        testTask.start(properties);

        SinkRecord spoofedRecord =
                spoofSinkRecord(topic, "k", "key", "v", "value", TimestampType.NO_TIMESTAMP_TYPE, null);
        testTask.put(Collections.singletonList(spoofedRecord));
        testTask.flush(Collections.emptyMap());
        verify(mockedStreamWriter, times(1)).append(any(JSONArray.class));
    }

    @Test
    public void testEmptyPut() {
        Map<String, String> properties = propertiesFactory.getProperties();
        properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");

        BigQuery bigQuery = mock(BigQuery.class);
        Storage storage = mock(Storage.class);

        SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        Map<TableId, Table> cache = new HashMap<>();

        BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager, cache, mockedStorageWriteApiDefaultStream);
        testTask.start(properties);

        testTask.put(Collections.emptyList());
    }

    // needed because debezium sends null messages when deleting messages in kafka
    @Test
    public void testEmptyRecordPut() {
        final String topic = "test_topic";
        final Schema simpleSchema = SchemaBuilder
                .struct()
                .field("aField", Schema.STRING_SCHEMA)
                .build();

        Map<String, String> properties = propertiesFactory.getProperties();
        properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");

        BigQuery bigQuery = mock(BigQuery.class);
        Storage storage = mock(Storage.class);

        SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        Map<TableId, Table> cache = new HashMap<>();

        BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager, cache, mockedStorageWriteApiDefaultStream);
        testTask.start(properties);

        SinkRecord emptyRecord = spoofSinkRecord(topic, simpleSchema, null);

        testTask.put(Collections.singletonList(emptyRecord));
    }

    // Throw an exception on the first put, and assert the Exception will be exposed in subsequent
    // put call.
    @Test(expected = BigQueryConnectException.class)
    public void testSimplePutException() throws Exception {
        final String topic = "test-topic";
        Map<String, String> properties = propertiesFactory.getProperties();
        properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
        properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
        properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");

        BigQuery bigQuery = mock(BigQuery.class);

        Storage storage = mock(Storage.class);
        SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        InterruptedException exception = new InterruptedException("I am non-retriable error");

        doReturn(mockedStreamWriter).when(mockedStorageWriteApiDefaultStream).getDefaultStream(any(), any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenThrow(exception);

        SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        Map<TableId, Table> cache = new HashMap<>();

        BigQuerySinkTask testTask =
                new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager, cache, mockedStorageWriteApiDefaultStream);
        testTask.initialize(sinkTaskContext);
        testTask.start(properties);

        testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
        try {
            while (true) {
                Thread.sleep(100);
                testTask.put(Collections.emptyList());
            }
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof BigQueryStorageWriteApiConnectException);
            throw e;
        }
    }

    @Test
    public void testEmptyFlush() {
        Map<String, String> properties = propertiesFactory.getProperties();
        properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");

        BigQuery bigQuery = mock(BigQuery.class);
        Storage storage = mock(Storage.class);

        SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        Map<TableId, Table> cache = new HashMap<>();

        SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
        BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager, cache, mockedStorageWriteApiDefaultStream);
        testTask.initialize(sinkTaskContext);
        testTask.start(properties);

        testTask.flush(Collections.emptyMap());
    }

    @Test
    public void testFlushAfterStop() throws Exception {
        Map<String, String> properties = propertiesFactory.getProperties();
        properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");

        Storage storage = mock(Storage.class);

        BigQuery bigQuery = mock(BigQuery.class);

        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        InterruptedException exception = new InterruptedException("I am non-retriable error");

        doReturn(mockedStreamWriter).when(mockedStorageWriteApiDefaultStream).getDefaultStream(any(), any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenThrow(exception);

        SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        Map<TableId, Table> cache = new HashMap<>();

        SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
        BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager, cache, mockedStorageWriteApiDefaultStream);
        testTask.initialize(sinkTaskContext);
        testTask.start(properties);

        testTask.put(Collections.singletonList(spoofSinkRecord("t")));
        assertThrows(
                "first call to flush should fail",
                Exception.class,
                () -> testTask.flush(Collections.emptyMap()));
        assertThrows(
                "second call to flush should fail",
                Exception.class,
                () -> testTask.flush(Collections.emptyMap()));
        testTask.stop();
        assertThrows(
                "third call to flush (after task stop) should fail",
                Exception.class,
                () -> testTask.flush(Collections.emptyMap()));
    }

    @Test
    public void testNoCallsToBigQueryReadTimeout() throws Exception {
        final String topic = "test_topic";
        final String dataset = "scratch";

        Map<String, String> properties = propertiesFactory.getProperties();
        properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
        properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG, "3");
        properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG, "2000");
        properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
        properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset);

        BigQuery bigQuery = mock(BigQuery.class);
        when(bigQuery.getTable(any())).thenThrow(new BigQueryException(new SocketTimeoutException("mock timeout")));

        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
                .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType()).build();

        doReturn(mockedStreamWriter).when(mockedStorageWriteApiDefaultStream).getDefaultStream(any(), any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenReturn(successResponse);

        Storage storage = mock(Storage.class);
        SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

        SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        Map<TableId, Table> cache = new HashMap<>();

        BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager, cache, mockedStorageWriteApiDefaultStream);
        testTask.initialize(sinkTaskContext);
        testTask.start(properties);
        testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
        testTask.flush(Collections.emptyMap());

        verify(bigQuery, times(0)).getTable(any());

    }

    @Test(expected = BigQueryConnectException.class)
    public void testBigQueryNonRetriable() throws Exception{
        final String topic = "test_topic";
        final String dataset = "scratch";

        Map<String, String> properties = propertiesFactory.getProperties();
        properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
        properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG, "2");
        properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG, "2000");
        properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
        properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset);

        BigQuery bigQuery = mock(BigQuery.class);

        Storage storage = mock(Storage.class);

        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        Map<Integer, String> errorMapping = new HashMap<>();
        errorMapping.put(18, "f0 field is unknown");
        Exceptions.AppendSerializtionError exception = new Exceptions.AppendSerializtionError(
                3,
                "Bad request",
                "DEFAULT",
                errorMapping);

        doReturn(mockedStreamWriter).when(mockedStorageWriteApiDefaultStream).getDefaultStream(any(), any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenThrow(exception);

        SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

        SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        Map<TableId, Table> cache = new HashMap<>();

        BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager, cache, mockedStorageWriteApiDefaultStream);
        testTask.initialize(sinkTaskContext);
        testTask.start(properties);
        testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
        testTask.flush(Collections.emptyMap());

        verify(mockedStreamWriter, times(1)).append(any(JSONArray.class));

        try {
            while (true) {
                Thread.sleep(100);
                testTask.put(Collections.emptyList());
            }
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof BigQueryStorageWriteApiConnectException);
            throw e;
        }
    }

    // Make sure that an InterruptedException is properly translated into a ConnectException
    @Test(expected = ConnectException.class)
    public void testInterruptedException() throws Exception{
        final String topic = "test_topic";
        final String dataset = "scratch";

        Map<String, String> properties = propertiesFactory.getProperties();
        properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
        properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
        properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset);

        BigQuery bigQuery = mock(BigQuery.class);

        Storage storage = mock(Storage.class);

        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
                .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType()).build();

        doReturn(mockedStreamWriter).when(mockedStorageWriteApiDefaultStream).getDefaultStream(any(), any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenReturn(successResponse);

        SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        Map<TableId, Table> cache = new HashMap<>();

        SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
        BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager, cache, mockedStorageWriteApiDefaultStream);
        testTask.initialize(sinkTaskContext);
        testTask.start(properties);

        testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
        testTask.flush(Collections.emptyMap());

        testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
        Thread.currentThread().interrupt();
        testTask.flush(Collections.emptyMap());
    }

    @Test
    public void testVersion() {
        assertNotNull(new BigQuerySinkTask().version());
    }

    // Existing tasks should succeed upon stop is called. New tasks should be rejected once task is
    // stopped.
    @Test(expected = RejectedExecutionException.class)
    public void testStop() throws Exception{
        final String topic = "test_topic";
        final String dataset = "scratch";

        Map<String, String> properties = propertiesFactory.getProperties();
        properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
        properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
        properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset);

        BigQuery bigQuery = mock(BigQuery.class);

        SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
                .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType()).build();

        doReturn(mockedStreamWriter).when(mockedStorageWriteApiDefaultStream).getDefaultStream(any(), any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenReturn(successResponse);

        SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        Map<TableId, Table> cache = new HashMap<>();

        Storage storage = mock(Storage.class);
        BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager, cache, mockedStorageWriteApiDefaultStream);
        testTask.initialize(sinkTaskContext);
        testTask.start(properties);
        testTask.put(Collections.singletonList(spoofSinkRecord(topic)));

        testTask.stop();

        verify(mockedStorageWriteApiDefaultStream, times(1)).shutdown();

        testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    }

    @Test
    public void testKafkaProviderConfigInvalidValue() {
        Map<String, String> configProperties = propertiesFactory.getProperties();
        configProperties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");

        String testKafkaProvider = "testProvider";
        configProperties.put(CONNECTOR_RUNTIME_PROVIDER_CONFIG, testKafkaProvider);
        BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);

        GcpClientBuilder<BigQuery> clientBuilder = new GcpClientBuilder.BigQueryBuilder().withConfig(config);
        assertTrue(clientBuilder.getHeaderProvider().getHeaders().get("user-agent").contains(CONNECTOR_RUNTIME_PROVIDER_DEFAULT));

        GcpClientBuilder<Storage> storageBuilder = new GcpClientBuilder.GcsBuilder().withConfig(config);
        assertTrue(storageBuilder.getHeaderProvider().getHeaders().get("user-agent").contains(CONNECTOR_RUNTIME_PROVIDER_DEFAULT));
    }

    /**
     * Utility method for spoofing SinkRecords that should be passed to SinkTask.put()
     *
     * @param topic         The topic of the record.
     * @param keyField      The field name for the record key; may be null.
     * @param key           The content of the record key; may be null.
     * @param valueField    The field name for the record value; may be null
     * @param value         The content of the record value; may be null
     * @param timestampType The type of timestamp embedded in the message
     * @param timestamp     The timestamp in milliseconds
     * @return The spoofed SinkRecord.
     */
    public static SinkRecord spoofSinkRecord(String topic, String keyField, String key,
                                             String valueField, String value,
                                             TimestampType timestampType, Long timestamp) {
        Schema basicKeySchema = null;
        Struct basicKey = null;
        if (keyField != null) {
            basicKeySchema = SchemaBuilder
                    .struct()
                    .field(keyField, Schema.STRING_SCHEMA)
                    .build();
            basicKey = new Struct(basicKeySchema);
            basicKey.put(keyField, key);
        }

        Schema basicValueSchema = null;
        Struct basicValue = null;
        if (valueField != null) {
            basicValueSchema = SchemaBuilder
                    .struct()
                    .field(valueField, Schema.STRING_SCHEMA)
                    .build();
            basicValue = new Struct(basicValueSchema);
            basicValue.put(valueField, value);
        }

        return new SinkRecord(topic, 0, basicKeySchema, basicKey,
                basicValueSchema, basicValue, spoofedRecordOffset.getAndIncrement(), timestamp, timestampType);
    }

    /**
     * Utility method for spoofing SinkRecords that should be passed to SinkTask.put()
     *
     * @param topic         The topic of the record.
     * @param field         The field name for the record value.
     * @param value         The content of the record value.
     * @param timestampType The type of timestamp embedded in the message
     * @param timestamp     The timestamp in milliseconds
     * @return The spoofed SinkRecord.
     */
    public static SinkRecord spoofSinkRecord(String topic, String field, String value,
                                             TimestampType timestampType, Long timestamp) {
        return spoofSinkRecord(topic, null, null, field, value, timestampType, timestamp);
    }

    /**
     * Utility method for spoofing SinkRecords that should be passed to SinkTask.put()
     *
     * @param topic       The topic of the record.
     * @param valueSchema The schema of the record.
     * @param value       The content of the record.
     * @return The spoofed SinkRecord.
     */
    public static SinkRecord spoofSinkRecord(String topic, Schema valueSchema, Struct value) {
        return new SinkRecord(topic, 0, null, null, valueSchema, value, 0);
    }

    /**
     * Utility method for spoofing SinkRecords that should be passed to SinkTask.put(). Creates a
     * record with a struct schema that contains only one string field with a given name and value.
     *
     * @param topic The topic of the record.
     * @param field The name of the field in the record's struct.
     * @param value The content of the field.
     * @return The spoofed SinkRecord.
     */
    public static SinkRecord spoofSinkRecord(String topic, String field, String value) {
        return spoofSinkRecord(topic, field, value, TimestampType.NO_TIMESTAMP_TYPE, null);
    }

    /**
     * Utility method for spoofing SinkRecords that should be passed to SinkTask.put(). Creates a
     * record with a struct schema that contains only one string field with a default name and a given
     * value.
     *
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
     *
     * @param topic The topic of the record.
     * @return The spoofed SinkRecord.
     */
    public static SinkRecord spoofSinkRecord(String topic) {
        return spoofSinkRecord(topic, "sink task test row");
    }
}
