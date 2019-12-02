package com.wepay.kafka.connect.bigquery.convert;


import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;

public class KafkaDataConverter {

    /* package private */ static final String KAFKA_DATA_FIELD_NAME = "kafkaData";
    /* package private */ static final String KAFKA_DATA_TOPIC_FIELD_NAME = "topic";
    /* package private */ static final String KAFKA_DATA_PARTITION_FIELD_NAME = "partition";
    /* package private */ static final String KAFKA_DATA_OFFSET_FIELD_NAME = "offset";
    /* package private */ static final String KAFKA_DATA_INSERT_TIME_FIELD_NAME = "insertTime";

    public static Field getKafkaDataField() {
        Field topicField = com.google.cloud.bigquery.Field.of(KAFKA_DATA_TOPIC_FIELD_NAME, LegacySQLTypeName.STRING);
        Field partitionField = com.google.cloud.bigquery.Field.of(KAFKA_DATA_PARTITION_FIELD_NAME, LegacySQLTypeName.INTEGER);
        Field offsetField = com.google.cloud.bigquery.Field.of(KAFKA_DATA_OFFSET_FIELD_NAME, LegacySQLTypeName.INTEGER);
        Field.Builder insertTimeBuilder = com.google.cloud.bigquery.Field.newBuilder(
                KAFKA_DATA_INSERT_TIME_FIELD_NAME, LegacySQLTypeName.TIMESTAMP)
                .setMode(com.google.cloud.bigquery.Field.Mode.NULLABLE);
        Field insertTimeField = insertTimeBuilder.build();

        return Field.newBuilder(KAFKA_DATA_FIELD_NAME, LegacySQLTypeName.RECORD,
                topicField, partitionField, offsetField, insertTimeField)
                .setMode(com.google.cloud.bigquery.Field.Mode.NULLABLE).build();
    }

    public static Map<String, Object> getKafkaDataRecord(SinkRecord kafkaConnectRecord) {
        HashMap<String, Object> kafkaData = new HashMap<>();
        kafkaData.put(KAFKA_DATA_TOPIC_FIELD_NAME, kafkaConnectRecord.topic());
        kafkaData.put(KAFKA_DATA_PARTITION_FIELD_NAME, kafkaConnectRecord.kafkaPartition());
        kafkaData.put(KAFKA_DATA_OFFSET_FIELD_NAME, kafkaConnectRecord.kafkaOffset());
        kafkaData.put(KAFKA_DATA_INSERT_TIME_FIELD_NAME, System.currentTimeMillis() / 1000.0);
        return kafkaData;
    }

}
