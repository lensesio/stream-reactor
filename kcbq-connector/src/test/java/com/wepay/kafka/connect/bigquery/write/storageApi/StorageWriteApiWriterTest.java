package com.wepay.kafka.connect.bigquery.write.storageApi;

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.BigQueryRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Optional;

public class StorageWriteApiWriterTest {
    Schema keySchema = SchemaBuilder.struct().field("key", Schema.STRING_SCHEMA).build();
    Schema valueSchema = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("available-name", Schema.BOOLEAN_SCHEMA)
            .build();

    @Test
    public void testRecordConversion() {
        StorageWriteApiBase mockStreamWriter = Mockito.mock(StorageWriteApiBase.class);
        BigQuerySinkTaskConfig mockedConfig = Mockito.mock(BigQuerySinkTaskConfig.class);
        StorageApiBatchModeHandler batchModeHandler = mock(StorageApiBatchModeHandler.class);
        RecordConverter mockedRecordConverter = new BigQueryRecordConverter(false, false);
        TableWriterBuilder builder = new StorageWriteApiWriter.Builder(
                mockStreamWriter, null, mockedRecordConverter, mockedConfig, batchModeHandler);
        ArgumentCaptor<List<Object[]>> records = ArgumentCaptor.forClass(List.class);
        String expectedKafkaKey = "{\"key\":\"12345\"}";
        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("id");
        expectedKeys.add("name");
        expectedKeys.add("available_name");
        expectedKeys.add("i_am_kafka_key");
        expectedKeys.add("i_am_kafka_record_detail");

        Mockito.when(mockedConfig.getKafkaDataFieldName()).thenReturn(Optional.of("i_am_kafka_record_detail"));
        Mockito.when(mockedConfig.getKafkaKeyFieldName()).thenReturn(Optional.of("i_am_kafka_key"));
        Mockito.when(mockedConfig.getBoolean(BigQuerySinkConfig.SANITIZE_FIELD_NAME_CONFIG)).thenReturn(true);

        builder.addRow(createRecord("abc", 100), null);
        builder.build().run();

        verify(mockStreamWriter, times(1))
                .initializeAndWriteRecords(any(), records.capture(), any());
        assertEquals(1, records.getValue().size());

        JSONObject actual = (JSONObject) records.getValue().get(0)[1];
        assertEquals(expectedKeys,actual.keySet());

        String actualKafkaKey = actual.get("i_am_kafka_key").toString();
        assertEquals(expectedKafkaKey, actualKafkaKey);

        JSONObject recordDetails = (JSONObject) actual.get("i_am_kafka_record_detail");
        assertTrue(recordDetails.get("insertTime") instanceof Long);
    }

    @Test
    public void testBatchLoadStreamName() {
        TableName tableName = TableName.of("p", "d", "t");
        StorageWriteApiBase mockStreamWriter = Mockito.mock(StorageWriteApiBatchApplicationStream.class);
        BigQuerySinkTaskConfig mockedConfig = Mockito.mock(BigQuerySinkTaskConfig.class);
        StorageApiBatchModeHandler batchModeHandler = mock(StorageApiBatchModeHandler.class);
        RecordConverter mockedRecordConverter = new BigQueryRecordConverter(
                false, false);
        ArgumentCaptor<String> streamName = ArgumentCaptor.forClass(String.class);
        String expectedStreamName = tableName.toString() + "_s1";
        TableWriterBuilder builder = new StorageWriteApiWriter.Builder(
                mockStreamWriter, tableName, mockedRecordConverter, mockedConfig, batchModeHandler);


        Mockito.when(mockedConfig.getKafkaDataFieldName()).thenReturn(Optional.empty());
        Mockito.when(mockedConfig.getKafkaKeyFieldName()).thenReturn(Optional.of("i_am_kafka_key"));
        Mockito.when(mockedConfig.getBoolean(BigQuerySinkConfig.SANITIZE_FIELD_NAME_CONFIG)).thenReturn(true);
        when(batchModeHandler.updateOffsetsOnStream(any(), any())).thenReturn(expectedStreamName);

        builder.addRow(createRecord("abc", 100), null);
        builder.build().run();

        verify(mockStreamWriter, times(1))
                .initializeAndWriteRecords(any(), any(), streamName.capture());

        assertEquals(expectedStreamName, streamName.getValue());


    }

    private SinkRecord createRecord(String topic, long offset) {
        Object key = new Struct(keySchema).put("key", "12345");
        Object value = new Struct(valueSchema)
                .put("id", 1L)
                .put("name", "1")
                .put("available-name", true);
        return new SinkRecord(topic, 0, keySchema, key, valueSchema, value, offset);
    }
}
