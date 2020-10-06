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

import static org.junit.Assert.assertEquals;

import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;

import org.junit.Test;

public class BigQuerySchemaConverterTest {

  @Test(expected = ConversionConnectException.class)
  public void testTopLevelSchema() {
    new BigQuerySchemaConverter().convertSchema(Schema.BOOLEAN_SCHEMA);
  }

  @Test
  public void testBoolean() {
    final String fieldName = "Boolean";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                com.google.cloud.bigquery.Field.Type.bool()
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.BOOLEAN_SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema =
        new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testInteger() {
    final String fieldName = "Integer";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                com.google.cloud.bigquery.Field.Type.integer()
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT8_SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema =
        new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);


    kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT16_SCHEMA)
        .build();

    bigQueryTestSchema = new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);


    kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT32_SCHEMA)
        .build();

    bigQueryTestSchema = new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);


    kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT64_SCHEMA)
        .build();

    bigQueryTestSchema = new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testFloat() {
    final String fieldName = "Float";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                com.google.cloud.bigquery.Field.Type.floatingPoint()
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.FLOAT32_SCHEMA)
        .build();
    com.google.cloud.bigquery.Schema bigQueryTestSchema =
        new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);

    kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.FLOAT64_SCHEMA)
        .build();

    bigQueryTestSchema =
        new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testString() {
    final String fieldName = "String";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                com.google.cloud.bigquery.Field.Type.string()
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.STRING_SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema =
        new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testStruct() { // Struct in a struct in a struct (wrapped in a struct)
    final String outerFieldStructName = "OuterStruct";
    final String middleFieldStructName = "MiddleStruct";
    final String middleFieldArrayName = "MiddleArray";
    final String innerFieldStructName = "InnerStruct";
    final String innerFieldStringName = "InnerString";
    final String innerFieldIntegerName = "InnerInt";

    com.google.cloud.bigquery.Field bigQueryInnerRecord =
        com.google.cloud.bigquery.Field.newBuilder(
            innerFieldStructName,
            com.google.cloud.bigquery.Field.Type.record(
                com.google.cloud.bigquery.Field.newBuilder(
                    innerFieldStringName,
                    com.google.cloud.bigquery.Field.Type.string()
                ).setMode(
                    com.google.cloud.bigquery.Field.Mode.REQUIRED
                ).build(),
                com.google.cloud.bigquery.Field.newBuilder(
                    innerFieldIntegerName,
                    com.google.cloud.bigquery.Field.Type.integer()
                ).setMode(
                    com.google.cloud.bigquery.Field.Mode.REQUIRED
                ).build()
            )
        ).setMode(
            com.google.cloud.bigquery.Field.Mode.REQUIRED
        ).build();

    Schema kafkaConnectInnerSchema = SchemaBuilder
        .struct()
        .field(innerFieldStringName, Schema.STRING_SCHEMA)
        .field(innerFieldIntegerName, Schema.INT32_SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryExpectedInnerSchema =
        com.google.cloud.bigquery.Schema.of(bigQueryInnerRecord);
    com.google.cloud.bigquery.Schema bigQueryTestInnerSchema =
        new BigQuerySchemaConverter().convertSchema(
            SchemaBuilder
                .struct()
                .field(innerFieldStructName, kafkaConnectInnerSchema)
                .build()
        );
    assertEquals(bigQueryExpectedInnerSchema, bigQueryTestInnerSchema);

    com.google.cloud.bigquery.Field bigQueryMiddleRecord =
        com.google.cloud.bigquery.Field.newBuilder(
            middleFieldStructName,
            com.google.cloud.bigquery.Field.Type.record(
                bigQueryInnerRecord,
                com.google.cloud.bigquery.Field.newBuilder(
                    middleFieldArrayName,
                    com.google.cloud.bigquery.Field.Type.floatingPoint()
                ).setMode(com.google.cloud.bigquery.Field.Mode.REPEATED).build()
            )
        ).setMode(
            com.google.cloud.bigquery.Field.Mode.REQUIRED
        ).build();

    Schema kafkaConnectMiddleSchema = SchemaBuilder
        .struct()
        .field(innerFieldStructName, kafkaConnectInnerSchema)
        .field(middleFieldArrayName, SchemaBuilder.array(Schema.FLOAT32_SCHEMA).build())
        .build();

    com.google.cloud.bigquery.Schema bigQueryExpectedMiddleSchema =
        com.google.cloud.bigquery.Schema.of(bigQueryMiddleRecord);
    com.google.cloud.bigquery.Schema bigQueryTestMiddleSchema =
        new BigQuerySchemaConverter().convertSchema(
            SchemaBuilder
                .struct()
                .field(middleFieldStructName, kafkaConnectMiddleSchema)
                .build()
        );
    assertEquals(bigQueryExpectedMiddleSchema, bigQueryTestMiddleSchema);

    com.google.cloud.bigquery.Field bigQueryOuterRecord =
        com.google.cloud.bigquery.Field.newBuilder(
            outerFieldStructName,
            com.google.cloud.bigquery.Field.Type.record(
                bigQueryInnerRecord,
                bigQueryMiddleRecord
            )
        ).setMode(
            com.google.cloud.bigquery.Field.Mode.REQUIRED
        ).build();

    Schema kafkaConnectOuterSchema = SchemaBuilder
        .struct()
        .field(innerFieldStructName, kafkaConnectInnerSchema)
        .field(middleFieldStructName, kafkaConnectMiddleSchema)
        .build();

    com.google.cloud.bigquery.Schema bigQueryExpectedOuterSchema =
        com.google.cloud.bigquery.Schema.of(bigQueryOuterRecord);
    com.google.cloud.bigquery.Schema bigQueryTestOuterSchema =
        new BigQuerySchemaConverter().convertSchema(
            SchemaBuilder
                .struct()
                .field(outerFieldStructName, kafkaConnectOuterSchema)
                .build()
        );
    assertEquals(bigQueryExpectedOuterSchema, bigQueryTestOuterSchema);
  }

  @Test
  public void testMap() {
    final String fieldName = "StringIntegerMap";
    final String keyName = BigQuerySchemaConverter.MAP_KEY_FIELD_NAME;
    final String valueName = BigQuerySchemaConverter.MAP_VALUE_FIELD_NAME;

    com.google.cloud.bigquery.Field.Type bigQueryMapEntryType =
        com.google.cloud.bigquery.Field.Type.record(
            com.google.cloud.bigquery.Field.newBuilder(
                keyName,
                com.google.cloud.bigquery.Field.Type.floatingPoint()
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build(),
            com.google.cloud.bigquery.Field.newBuilder(
                valueName,
                com.google.cloud.bigquery.Field.Type.string()
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                bigQueryMapEntryType
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REPEATED
            ).build()
        );


    Schema kafkaConnectMapSchema = SchemaBuilder
        .map(Schema.FLOAT32_SCHEMA, Schema.STRING_SCHEMA)
        .build();
    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, kafkaConnectMapSchema)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema =
        new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testIntegerArray() {
    final String fieldName = "IntegerArray";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                com.google.cloud.bigquery.Field.Type.integer()
            ).setMode(com.google.cloud.bigquery.Field.Mode.REPEATED).build()
        );

    Schema kafkaConnectArraySchema = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, kafkaConnectArraySchema)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema =
        new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testStringArray() {
    final String fieldName = "StringArray";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                com.google.cloud.bigquery.Field.Type.string()
            ).setMode(com.google.cloud.bigquery.Field.Mode.REPEATED).build()
        );

    Schema kafkaConnectArraySchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, kafkaConnectArraySchema)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema =
        new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testBytes() {
    final String fieldName = "Bytes";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                com.google.cloud.bigquery.Field.Type.bytes()
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.BYTES_SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema =
        new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testTimestamp() {
    final String fieldName = "Timestamp";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                com.google.cloud.bigquery.Field.Type.timestamp()
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Timestamp.SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema =
        new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test(expected = ConversionConnectException.class)
  public void testBadTimestamp() {
    final String fieldName = "Timestamp";

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, SchemaBuilder.bool().name(Timestamp.LOGICAL_NAME))
        .build();

    new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
  }

  @Test
  public void testDate() {
    final String fieldName = "Date";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                com.google.cloud.bigquery.Field.Type.date()
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Date.SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema =
        new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test(expected = ConversionConnectException.class)
  public void testBadDate() {
    final String fieldName = "Date";

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, SchemaBuilder.int64().name(Date.LOGICAL_NAME))
        .build();

    new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
  }

  @Test
  public void testDecimal() {
    final String fieldName = "Decimal";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                com.google.cloud.bigquery.Field.Type.floatingPoint()
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Decimal.schema(0))
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema =
        new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test(expected = ConversionConnectException.class)
  public void testBadDecimal() {
    final String fieldName = "Decimal";

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, SchemaBuilder.bool().name(Decimal.LOGICAL_NAME))
        .build();

    new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
  }

  @Test
  public void testNullable() {
    final String nullableFieldName = "Nullable";
    final String requiredFieldName = "Required";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                nullableFieldName,
                com.google.cloud.bigquery.Field.Type.integer()
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.NULLABLE
            ).build(),
            com.google.cloud.bigquery.Field.newBuilder(
                requiredFieldName,
                com.google.cloud.bigquery.Field.Type.integer()
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(nullableFieldName, SchemaBuilder.int32().optional().build())
        .field(requiredFieldName, SchemaBuilder.int32().required().build())
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema =
        new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testDescription() {
    final String fieldName = "WithDoc";
    final String fieldDoc = "test documentation";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(fieldName,
                com.google.cloud.bigquery.Field.Type.string())
                .setMode(com.google.cloud.bigquery.Field.Mode.REQUIRED)
                .setDescription(fieldDoc)
                .build()
        );

    Schema kafkaConnectTestSchema =
        SchemaBuilder.struct()
                     .field(fieldName, SchemaBuilder.string().doc(fieldDoc).build())
                     .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema =
        new BigQuerySchemaConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }
}
