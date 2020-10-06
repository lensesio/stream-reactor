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

import com.google.cloud.bigquery.Field;

import com.wepay.kafka.connect.bigquery.convert.BigQuerySchemaConverter;

import org.apache.kafka.connect.data.Schema;

/**
 * Class for converting from {@link Schema Kafka Connect Schemas} to
 * {@link com.google.cloud.bigquery.Schema BigQuery Schemas}, but adds an extra
 * kafkaData field containing topic, partition, and offset information in the
 * resulting BigQuery Schema.
 */
public class KafkaDataBQSchemaConverter extends BigQuerySchemaConverter {

  /* package private */ static final String KAFKA_DATA_FIELD_NAME = "kafkaData";
  /* package private */ static final String KAFKA_DATA_TOPIC_FIELD_NAME = "topic";
  /* package private */ static final String KAFKA_DATA_PARTITION_FIELD_NAME = "partition";
  /* package private */ static final String KAFKA_DATA_OFFSET_FIELD_NAME = "offset";
  /* package private */ static final String KAFKA_DATA_INSERT_TIME_FIELD_NAME = "insertTime";

  /**
   * Convert the  kafka {@link Schema} to a BigQuery {@link com.google.cloud.bigquery.Schema}, with
   * the addition of an optional field for containing extra kafka data.
   *
   * @param kafkaConnectSchema The schema to convert. Must be of type Struct, in order to translate
   *                           into a row format that requires each field to consist of both a name
   *                           and a value.
   * @return the converted {@link com.google.cloud.bigquery.Schema}, including an extra optional
   *         field for the kafka topic, partition, and offset.
   */
  public com.google.cloud.bigquery.Schema convertSchema(Schema kafkaConnectSchema) {
    com.google.cloud.bigquery.Schema.Builder schemaBuilder =
        super.convertSchema(kafkaConnectSchema).toBuilder();

    Field topicField = Field.of(KAFKA_DATA_TOPIC_FIELD_NAME, Field.Type.string());
    Field partitionField = Field.of(KAFKA_DATA_PARTITION_FIELD_NAME, Field.Type.integer());
    Field offsetField = Field.of(KAFKA_DATA_OFFSET_FIELD_NAME, Field.Type.integer());
    Field.Builder insertTimeBuilder = Field.newBuilder(KAFKA_DATA_INSERT_TIME_FIELD_NAME,
                                                    Field.Type.timestamp())
                                           .setMode(Field.Mode.NULLABLE);
    Field insertTimeField = insertTimeBuilder.build();

    Field.Builder kafkaDataFieldBuilder =
        Field.newBuilder(KAFKA_DATA_FIELD_NAME, Field.Type.record(topicField,
                                                               partitionField,
                                                               offsetField,
                                                               insertTimeField))
             .setMode(Field.Mode.NULLABLE);

    schemaBuilder.addField(kafkaDataFieldBuilder.build());

    return schemaBuilder.build();
  }
}
