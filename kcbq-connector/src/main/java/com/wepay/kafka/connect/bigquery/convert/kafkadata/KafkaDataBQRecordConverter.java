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

import com.wepay.kafka.connect.bigquery.convert.BigQueryRecordConverter;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Class for converting from {@link SinkRecord SinkRecords} and BigQuery rows, which are represented
 * as {@link Map Maps} from {@link String Strings} to {@link Object Objects}, but adds an extra
 * kafkaData field containing topic, partition, and offset information in the
 * resulting BigQuery Schema.
 *
 * <p>This converter will only work when used with the associated KafkaDAtaBQSchemaConverter.
 */
public class KafkaDataBQRecordConverter extends BigQueryRecordConverter {

  /**
   * Convert the kafka {@link SinkRecord} to a BigQuery record, with the addition of extra kafka
   * data.
   *
   * @param kafkaConnectRecord The Kafka Connect record to convert. Must be of type
   *                           {@link org.apache.kafka.connect.data.Struct}, in order
   *                           to translate into a row format that requires each field to
   *                           consist of both a name and a value.
   * @return converted BigQuery record including the kafka topic, partition, and offset.
   */
  public Map<String, Object> convertRecord(SinkRecord kafkaConnectRecord) {
    Map<String, Object> record = super.convertRecord(kafkaConnectRecord);
    HashMap<String, Object> kafkaData = new HashMap<>();
    record.put(KafkaDataBQSchemaConverter.KAFKA_DATA_FIELD_NAME, kafkaData);
    kafkaData.put(KafkaDataBQSchemaConverter.KAFKA_DATA_TOPIC_FIELD_NAME,
                  kafkaConnectRecord.topic());
    kafkaData.put(KafkaDataBQSchemaConverter.KAFKA_DATA_PARTITION_FIELD_NAME,
                  kafkaConnectRecord.kafkaPartition());
    kafkaData.put(KafkaDataBQSchemaConverter.KAFKA_DATA_OFFSET_FIELD_NAME,
                  kafkaConnectRecord.kafkaOffset());
    kafkaData.put(KafkaDataBQSchemaConverter.KAFKA_DATA_INSERT_TIME_FIELD_NAME,
                  System.currentTimeMillis() / 1000.0);
    return record;
  }
}
