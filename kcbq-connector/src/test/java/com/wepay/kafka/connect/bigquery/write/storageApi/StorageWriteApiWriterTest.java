package com.wepay.kafka.connect.bigquery.write.storageApi;

import static org.junit.Assert.assertEquals;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
        RecordConverter mockedRecordConverter = new BigQueryRecordConverter(false, false);
        TableWriterBuilder builder = new StorageWriteApiWriter.Builder(mockStreamWriter, null, mockedRecordConverter, mockedConfig);
        ArgumentCaptor<List<Object[]>> records = ArgumentCaptor.forClass(List.class);
        String expectedKafkaKey = "{\"key\":\"12345\"}";
        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("id");
        expectedKeys.add("name");
        expectedKeys.add("available_name");
        expectedKeys.add("i_am_kafka_key");


        Mockito.when(mockedConfig.getKafkaDataFieldName()).thenReturn(Optional.empty());
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
    }

    private SinkRecord createRecord(String topic, long offset) {
        Object key = new Struct(keySchema).put("key", "12345");
        Object value =new Struct(valueSchema)
                .put("id", 1L)
                .put("name", "1")
                .put("available-name", true);
        return new SinkRecord(topic, 0, keySchema, key, valueSchema, value, offset);
    }
}
