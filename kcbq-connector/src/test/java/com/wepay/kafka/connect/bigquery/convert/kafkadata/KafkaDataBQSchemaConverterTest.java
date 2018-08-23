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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.junit.Test;

public class KafkaDataBQSchemaConverterTest {

  @Test
  public void test() {
    Schema kafkaConnectTestSchema =
        SchemaBuilder.struct().field("base", Schema.STRING_SCHEMA).build();


    Field kafkaDataField = getKafkaDataField();
    Field baseField = Field.newBuilder("base",
                                    LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build();
    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(baseField, kafkaDataField);

    com.google.cloud.bigquery.Schema bigQueryActualSchema =
        new KafkaDataBQSchemaConverter(false).convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryActualSchema);
  }

  private Field getKafkaDataField() {
    Field topicField = Field.of("topic", LegacySQLTypeName.STRING);
    Field partitionField = Field.of("partition", LegacySQLTypeName.INTEGER);
    Field offsetField = Field.of("offset", LegacySQLTypeName.INTEGER);
    Field insertTimeField = Field.newBuilder("insertTime",LegacySQLTypeName.TIMESTAMP)
                                 .setMode(Field.Mode.NULLABLE)
                                 .build();

    return Field.newBuilder("kafkaData",
                            LegacySQLTypeName.RECORD,
                            topicField,
                            partitionField,
                            offsetField,
                            insertTimeField)
                .setMode(Field.Mode.NULLABLE)
                .build();
  }
}
