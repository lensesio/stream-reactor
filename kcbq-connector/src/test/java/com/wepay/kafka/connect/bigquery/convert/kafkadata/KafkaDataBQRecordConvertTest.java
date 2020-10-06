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

package com.wepay.kafka.connect.bigquery.convert.kafkadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaDataBQRecordConvertTest {

  private static final String baseFieldName = "base";
  private static final String kafkaDataFieldName = "kafkaData";

  private static final String kafkaDataTopicName = "topic";
  private static final String kafkaDataPartitionName = "partition";
  private static final String kafkaDataOffsetName = "offset";
  private static final String kafkaDataInsertTimeName = "insertTime";

  @Test
  public void test() {
    final String baseFieldValue = "a value!";

    final String kafkaDataTopicValue = "testTopic";
    final int kafkaDataPartitionValue = 101;
    final long kafkaDataOffsetValue = 1337;

    Map<String, Object> kafkaDataBQFieldValue = new HashMap<>();
    kafkaDataBQFieldValue.put(kafkaDataTopicName, kafkaDataTopicValue);
    kafkaDataBQFieldValue.put(kafkaDataPartitionName, kafkaDataPartitionValue);
    kafkaDataBQFieldValue.put(kafkaDataOffsetName, kafkaDataOffsetValue);

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(baseFieldName, baseFieldValue);
    bigQueryExpectedRecord.put(kafkaDataFieldName, kafkaDataBQFieldValue);

    Schema kafkaConnectSchema = SchemaBuilder.struct().field(baseFieldName, Schema.STRING_SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(baseFieldName, baseFieldValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema,
                                                    kafkaConnectStruct,
                                                    kafkaDataTopicValue,
                                                    kafkaDataPartitionValue,
                                                    kafkaDataOffsetValue);
    Map<String, Object> bigQueryActualRecord =
        new KafkaDataBQRecordConverter().convertRecord(kafkaConnectRecord);
    checkRecord(bigQueryExpectedRecord, bigQueryActualRecord);
  }

  private static SinkRecord spoofSinkRecord(Schema valueSchema,
                                            Object value,
                                            String topic,
                                            int partition,
                                            long offset) {
    return new SinkRecord(topic, partition, null, null, valueSchema, value, offset);
  }

  private static void checkRecord(Map<String, Object> partialExpectedRecord,
                                  Map<String, Object> actualRecord) {
    // we can't reasonably check the value of insertTime,
    // so we'll just check if it's there and is the correct type.
    @SuppressWarnings("unchecked")
    Map<String, Object> kafkaDataMap = (Map<String, Object>) actualRecord.get(kafkaDataFieldName);
    assertTrue(kafkaDataMap.containsKey(kafkaDataInsertTimeName));
    assertTrue(kafkaDataMap.get(kafkaDataInsertTimeName) instanceof Double);
    kafkaDataMap.remove(kafkaDataInsertTimeName);
    actualRecord.put(kafkaDataFieldName, kafkaDataMap);
    assertEquals(partialExpectedRecord, actualRecord);
  }
}
