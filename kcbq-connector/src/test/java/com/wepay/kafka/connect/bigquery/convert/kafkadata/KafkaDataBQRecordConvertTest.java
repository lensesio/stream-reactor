package com.wepay.kafka.connect.bigquery.convert.kafkadata;

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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaDataBQRecordConvertTest {

  @Test
  public void test() {

    final String baseFieldName = "base";
    final String baseFieldValue = "a value!";

    final String kafkaDataTopicName = "topic";
    final String kafkaDataTopicValue = "testTopic";
    final String kafkaDataPartitionName = "partition";
    final int kafkaDataPartitionValue = 101;
    final String kafkaDataOffsetName = "offset";
    final long kafkaDataOffsetValue = 1337;
    final String kafkaDataFieldName = "kafkaData";

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
    assertEquals(bigQueryExpectedRecord, bigQueryActualRecord);
  }

  private static SinkRecord spoofSinkRecord(Schema valueSchema,
                                            Object value,
                                            String topic,
                                            int partition,
                                            long offset) {
    return new SinkRecord(topic, partition, null, null, valueSchema, value, offset);
  }
}
