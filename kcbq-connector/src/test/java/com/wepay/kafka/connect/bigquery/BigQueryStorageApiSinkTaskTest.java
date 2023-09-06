package com.wepay.kafka.connect.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.Table;

import com.google.cloud.storage.Storage;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.write.storageApi.StorageApiBatchModeHandler;
import com.wepay.kafka.connect.bigquery.write.storageApi.StorageWriteApiDefaultStream;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static com.wepay.kafka.connect.bigquery.write.storageApi.StorageWriteApiWriter.DEFAULT;

public class BigQueryStorageApiSinkTaskTest {
    private static final SinkTaskPropertiesFactory propertiesFactory = new SinkTaskPropertiesFactory();
    Map<String, String> properties;
    private static AtomicLong spoofedRecordOffset = new AtomicLong();
    private static StorageWriteApiDefaultStream mockedStorageWriteApiDefaultStream = mock(
            StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
    ;
    final String topic = "test_topic";
    BigQuery bigQuery = mock(BigQuery.class);

    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Map<TableId, Table> cache = new HashMap<>();
    StorageApiBatchModeHandler storageApiBatchHandler = mock(StorageApiBatchModeHandler.class);
    BigQuerySinkTask testTask = new BigQuerySinkTask(
            bigQuery, schemaRetriever, storage, schemaManager, cache, mockedStorageWriteApiDefaultStream, storageApiBatchHandler);

    @Before
    public void setUp() {
        spoofedRecordOffset.set(0);
        properties = propertiesFactory.getProperties();

        properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
        properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
        properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
        spoofedRecordOffset.set(0);

        doNothing().when(mockedStorageWriteApiDefaultStream).appendRows(any(), any(), eq(DEFAULT));
        doNothing().when(mockedStorageWriteApiDefaultStream).shutdown();

        testTask.initialize(sinkTaskContext);
        testTask.start(properties);
    }

    @Test
    public void testPut() {
        testTask.put(Collections.singletonList(spoofSinkRecord()));
        testTask.flush(Collections.emptyMap());

        verify(mockedStorageWriteApiDefaultStream, times(1)).appendRows(any(), any(), eq(DEFAULT));
    }

    @Test(expected = BigQueryConnectException.class)
    public void testSimplePutException() throws Exception {
        BigQueryStorageWriteApiConnectException exception = new BigQueryStorageWriteApiConnectException("error 12345");

        doThrow(exception).when(mockedStorageWriteApiDefaultStream).appendRows(any(), any(),eq(DEFAULT));

        testTask.put(Collections.singletonList(spoofSinkRecord()));
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

    @Test(expected = RejectedExecutionException.class)
    public void testStop() {
        testTask.stop();

        verify(mockedStorageWriteApiDefaultStream, times(1)).shutdown();

        testTask.put(Collections.singletonList(spoofSinkRecord()));
    }

    private SinkRecord spoofSinkRecord() {

        Schema basicValueSchema = SchemaBuilder
                .struct()
                .field("sink_task_test_field", Schema.STRING_SCHEMA)
                .build();
        Struct basicValue = new Struct(basicValueSchema);
        basicValue.put("sink_task_test_field", "sink task test row");


        return new SinkRecord(topic, 0, null, null,
                basicValueSchema, basicValue, spoofedRecordOffset.getAndIncrement(), null, TimestampType.NO_TIMESTAMP_TYPE);
    }
}
