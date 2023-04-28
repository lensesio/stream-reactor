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

package com.wepay.kafka.connect.bigquery.convert;


import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
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
    Map<String, Object> expectedKafkaDataFields = new HashMap<>();
    private static final String kafkaDataTopicValue = "testTopic";
    private static final int kafkaDataPartitionValue = 101;
    private static final long kafkaDataOffsetValue = 1337;

    @Before
    public void setup() {
        expectedKafkaDataFields.put(kafkaDataTopicName, kafkaDataTopicValue);
        expectedKafkaDataFields.put(kafkaDataPartitionName, kafkaDataPartitionValue);
        expectedKafkaDataFields.put(kafkaDataOffsetName, kafkaDataOffsetValue);
    }

    @Test
    public void testBuildKafkaDataRecord() {
        SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
        Map<String, Object> actualKafkaDataFields = KafkaDataBuilder.buildKafkaDataRecord(record);

        assertTrue(actualKafkaDataFields.containsKey(kafkaDataInsertTimeName));
        assertTrue(actualKafkaDataFields.get(kafkaDataInsertTimeName) instanceof Double);

        actualKafkaDataFields.remove(kafkaDataInsertTimeName);

        assertEquals(expectedKafkaDataFields, actualKafkaDataFields);
    }

    @Test
    public void testBuildKafkaDataRecordStorageWriteApi() {
        SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
        Map<String, Object> actualKafkaDataFields = KafkaDataBuilder.buildKafkaDataRecordStorageApi(record);

        assertTrue(actualKafkaDataFields.containsKey(kafkaDataInsertTimeName));
        assertTrue(actualKafkaDataFields.get(kafkaDataInsertTimeName) instanceof Long);

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
