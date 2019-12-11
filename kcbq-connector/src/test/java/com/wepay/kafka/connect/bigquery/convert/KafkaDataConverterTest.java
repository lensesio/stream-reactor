package com.wepay.kafka.connect.bigquery.convert;


import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaDataConverterTest {

    public static final String kafkaDataFieldName = "kafkaData";
    private static final String kafkaDataTopicName = "topic";
    private static final String kafkaDataPartitionName = "partition";
    private static final String kafkaDataOffsetName = "offset";
    private static final String kafkaDataInsertTimeName = "insertTime";

    @Test
    public void testBuildKafkaDataRecord() {

        final String kafkaDataTopicValue = "testTopic";
        final int kafkaDataPartitionValue = 101;
        final long kafkaDataOffsetValue = 1337;

        Map<String, Object> expectedKafkaDataFields = new HashMap<>();
        expectedKafkaDataFields.put(kafkaDataTopicName, kafkaDataTopicValue);
        expectedKafkaDataFields.put(kafkaDataPartitionName, kafkaDataPartitionValue);
        expectedKafkaDataFields.put(kafkaDataOffsetName, kafkaDataOffsetValue);

        SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
        Map<String, Object> actualKafkaDataFields = KafkaDataBuilder.buildKafkaDataRecord(record);

        assertTrue(actualKafkaDataFields.containsKey(kafkaDataInsertTimeName));
        assertTrue(actualKafkaDataFields.get(kafkaDataInsertTimeName) instanceof Double);

        actualKafkaDataFields.remove(kafkaDataInsertTimeName);

        assertEquals(expectedKafkaDataFields, actualKafkaDataFields);
    }

    @Test
    public void testBuildKafkaDataField() {
        Field topicField = Field.of("topic", LegacySQLTypeName.STRING);
        Field partitionField = Field.of("partition", LegacySQLTypeName.INTEGER);
        Field offsetField = Field.of("offset", LegacySQLTypeName.INTEGER);
        Field insertTimeField = Field.newBuilder("insertTime",LegacySQLTypeName.TIMESTAMP)
                .setMode(Field.Mode.NULLABLE)
                .build();

        Field expectedBigQuerySchema = Field.newBuilder(kafkaDataFieldName,
                LegacySQLTypeName.RECORD,
                topicField,
                partitionField,
                offsetField,
                insertTimeField)
                .setMode(Field.Mode.NULLABLE)
                .build();
        Field actualBigQuerySchema = KafkaDataBuilder.buildKafkaDataField(kafkaDataFieldName);
        assertEquals(expectedBigQuerySchema, actualBigQuerySchema);
    }
}
