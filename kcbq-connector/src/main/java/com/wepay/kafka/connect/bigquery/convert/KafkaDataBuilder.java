package com.wepay.kafka.connect.bigquery.convert;


import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to construct schema and record for Kafka Data Field.
 */
public class KafkaDataBuilder {

    public static final String KAFKA_DATA_TOPIC_FIELD_NAME = "topic";
    public static final String KAFKA_DATA_PARTITION_FIELD_NAME = "partition";
    public static final String KAFKA_DATA_OFFSET_FIELD_NAME = "offset";
    public static final String KAFKA_DATA_INSERT_TIME_FIELD_NAME = "insertTime";

    /**
     * Construct schema for Kafka Data Field
     *
     * @param kafkaDataFieldName The configured name of Kafka Data Field
     * @return Field of Kafka Data, with definitions of kafka topic, partition, offset, and insertTime.
     */
    public static Field buildKafkaDataField(String kafkaDataFieldName) {
        Field topicField = com.google.cloud.bigquery.Field.of(KAFKA_DATA_TOPIC_FIELD_NAME, LegacySQLTypeName.STRING);
        Field partitionField = com.google.cloud.bigquery.Field.of(KAFKA_DATA_PARTITION_FIELD_NAME, LegacySQLTypeName.INTEGER);
        Field offsetField = com.google.cloud.bigquery.Field.of(KAFKA_DATA_OFFSET_FIELD_NAME, LegacySQLTypeName.INTEGER);
        Field.Builder insertTimeBuilder = com.google.cloud.bigquery.Field.newBuilder(
                KAFKA_DATA_INSERT_TIME_FIELD_NAME, LegacySQLTypeName.TIMESTAMP)
                .setMode(com.google.cloud.bigquery.Field.Mode.NULLABLE);
        Field insertTimeField = insertTimeBuilder.build();

        return Field.newBuilder(kafkaDataFieldName, LegacySQLTypeName.RECORD,
                topicField, partitionField, offsetField, insertTimeField)
                .setMode(com.google.cloud.bigquery.Field.Mode.NULLABLE).build();
    }

    /**
     * Construct a map of Kafka Data record
     *
     * @param kafkaConnectRecord Kafka sink record to build kafka data from.
     * @return HashMap which contains the values of kafka topic, partition, offset, and insertTime.
     */
    public static Map<String, Object> buildKafkaDataRecord(SinkRecord kafkaConnectRecord) {
        HashMap<String, Object> kafkaData = new HashMap<>();
        kafkaData.put(KAFKA_DATA_TOPIC_FIELD_NAME, kafkaConnectRecord.topic());
        kafkaData.put(KAFKA_DATA_PARTITION_FIELD_NAME, kafkaConnectRecord.kafkaPartition());
        kafkaData.put(KAFKA_DATA_OFFSET_FIELD_NAME, kafkaConnectRecord.kafkaOffset());
        kafkaData.put(KAFKA_DATA_INSERT_TIME_FIELD_NAME, System.currentTimeMillis() / 1000.0);
        return kafkaData;
    }

}
