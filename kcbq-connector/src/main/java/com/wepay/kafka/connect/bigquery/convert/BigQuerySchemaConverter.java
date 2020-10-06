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

import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.LogicalConverterRegistry;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.LogicalTypeConverter;
import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;

import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Time;
import io.debezium.time.ZonedTimestamp;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for converting from {@link Schema Kafka Connect Schemas} to
 * {@link com.google.cloud.bigquery.Schema BigQuery Schemas}.
 */
public class BigQuerySchemaConverter implements SchemaConverter<com.google.cloud.bigquery.Schema> {

  /**
   * The name of the field that contains keys from a converted Kafka Connect map.
   */
  public static final String MAP_KEY_FIELD_NAME = "key";

  /**
   * The name of the field that contains values keys from a converted Kafka Connect map.
   */
  public static final String MAP_VALUE_FIELD_NAME = "value";

  private static final Map<Schema.Type, com.google.cloud.bigquery.Field.Type> PRIMITIVE_TYPE_MAP;

  static {
    // force registration
    new DebeziumLogicalConverters();
    new KafkaLogicalConverters();

    PRIMITIVE_TYPE_MAP = new HashMap<>();
    PRIMITIVE_TYPE_MAP.put(Schema.Type.BOOLEAN,
                           com.google.cloud.bigquery.Field.Type.bool());
    PRIMITIVE_TYPE_MAP.put(Schema.Type.FLOAT32,
                           com.google.cloud.bigquery.Field.Type.floatingPoint());
    PRIMITIVE_TYPE_MAP.put(Schema.Type.FLOAT64,
                           com.google.cloud.bigquery.Field.Type.floatingPoint());
    PRIMITIVE_TYPE_MAP.put(Schema.Type.INT8,
                           com.google.cloud.bigquery.Field.Type.integer());
    PRIMITIVE_TYPE_MAP.put(Schema.Type.INT16,
                           com.google.cloud.bigquery.Field.Type.integer());
    PRIMITIVE_TYPE_MAP.put(Schema.Type.INT32,
                           com.google.cloud.bigquery.Field.Type.integer());
    PRIMITIVE_TYPE_MAP.put(Schema.Type.INT64,
                           com.google.cloud.bigquery.Field.Type.integer());
    PRIMITIVE_TYPE_MAP.put(Schema.Type.STRING,
                           com.google.cloud.bigquery.Field.Type.string());
    PRIMITIVE_TYPE_MAP.put(Schema.Type.BYTES,
                           com.google.cloud.bigquery.Field.Type.bytes());
  }

  /**
   * Convert a {@link Schema Kafka Connect Schema} into a
   * {@link com.google.cloud.bigquery.Schema BigQuery schema}.
   *
   * @param kafkaConnectSchema The schema to convert. Must be of type Struct, in order to translate
   *                           into a row format that requires each field to consist of both a name
   *                           and a value.
   * @return The resulting schema, which can then be used to create a new table or update an
   *         existing one.
   */
  public com.google.cloud.bigquery.Schema convertSchema(Schema kafkaConnectSchema) {
    if (kafkaConnectSchema.type() != Schema.Type.STRUCT) {
      throw new
          ConversionConnectException("Top-level Kafka Connect schema must be of type 'struct'");
    }
    com.google.cloud.bigquery.Schema.Builder bigQuerySchemaBuilder =
        com.google.cloud.bigquery.Schema.newBuilder();
    for (Field kafkaConnectField : kafkaConnectSchema.fields()) {
      com.google.cloud.bigquery.Field.Builder bigQuerySchemaFieldBuilder =
          convertField(kafkaConnectField.schema(), kafkaConnectField.name());
      bigQuerySchemaBuilder.addField(bigQuerySchemaFieldBuilder.build());
    }
    return bigQuerySchemaBuilder.build();
  }

  private com.google.cloud.bigquery.Field.Builder convertField(Schema kafkaConnectSchema,
                                                               String fieldName) {
    com.google.cloud.bigquery.Field.Builder result;
    Schema.Type kafkaConnectSchemaType = kafkaConnectSchema.type();
    if (LogicalConverterRegistry.isRegisteredLogicalType(kafkaConnectSchema.name())) {
      result = convertLogical(kafkaConnectSchema, fieldName);
    } else if (PRIMITIVE_TYPE_MAP.containsKey(kafkaConnectSchemaType)) {
      result = convertPrimitive(kafkaConnectSchema, fieldName);
    } else {
      switch (kafkaConnectSchemaType) {
        case STRUCT:
          result = convertStruct(kafkaConnectSchema, fieldName);
          break;
        case ARRAY:
          result = convertArray(kafkaConnectSchema, fieldName);
          break;
        case MAP:
          result = convertMap(kafkaConnectSchema, fieldName);
          break;
        default:
          throw new ConversionConnectException(
              "Unrecognized schema type: " + kafkaConnectSchemaType
          );
      }
    }
    setNullability(kafkaConnectSchema, result);
    if (kafkaConnectSchema.doc() != null) {
      result.setDescription(kafkaConnectSchema.doc());
    }
    return result;
  }

  private void setNullability(Schema kafkaConnectSchema,
                              com.google.cloud.bigquery.Field.Builder fieldBuilder) {
    switch (kafkaConnectSchema.type()) {
      case ARRAY:
      case MAP:
        return;
      default:
        if (kafkaConnectSchema.isOptional()) {
          fieldBuilder.setMode(com.google.cloud.bigquery.Field.Mode.NULLABLE);
        } else {
          fieldBuilder.setMode(com.google.cloud.bigquery.Field.Mode.REQUIRED);
        }
    }
  }

  private com.google.cloud.bigquery.Field.Builder convertStruct(Schema kafkaConnectSchema,
                                                                String fieldName) {
    List<com.google.cloud.bigquery.Field> bigQueryRecordFields = new ArrayList<>();
    for (Field kafkaConnectField : kafkaConnectSchema.fields()) {
      com.google.cloud.bigquery.Field.Builder bigQueryRecordFieldBuilder =
          convertField(kafkaConnectField.schema(), kafkaConnectField.name());
      bigQueryRecordFields.add(bigQueryRecordFieldBuilder.build());
    }
    com.google.cloud.bigquery.Field.Type bigQueryRecordType =
        com.google.cloud.bigquery.Field.Type.record(bigQueryRecordFields);
    return com.google.cloud.bigquery.Field.newBuilder(fieldName, bigQueryRecordType);
  }

  private com.google.cloud.bigquery.Field.Builder convertArray(Schema kafkaConnectSchema,
                                                               String fieldName) {
    Schema elementSchema = kafkaConnectSchema.valueSchema();
    com.google.cloud.bigquery.Field.Builder elementFieldBuilder =
        convertField(elementSchema, fieldName);
    return elementFieldBuilder.setMode(com.google.cloud.bigquery.Field.Mode.REPEATED);
  }

  private com.google.cloud.bigquery.Field.Builder convertMap(Schema kafkaConnectSchema,
                                                             String fieldName) {
    Schema keySchema = kafkaConnectSchema.keySchema();
    Schema valueSchema = kafkaConnectSchema.valueSchema();

    com.google.cloud.bigquery.Field.Builder keyFieldBuilder =
        convertField(keySchema, MAP_KEY_FIELD_NAME);
    com.google.cloud.bigquery.Field.Builder valueFieldBuilder =
        convertField(valueSchema, MAP_VALUE_FIELD_NAME);

    com.google.cloud.bigquery.Field keyField = keyFieldBuilder.build();
    com.google.cloud.bigquery.Field valueField = valueFieldBuilder.build();

    com.google.cloud.bigquery.Field.Type bigQueryMapEntryType =
        com.google.cloud.bigquery.Field.Type.record(keyField, valueField);

    com.google.cloud.bigquery.Field.Builder bigQueryRecordBuilder =
        com.google.cloud.bigquery.Field.newBuilder(fieldName, bigQueryMapEntryType);

    return bigQueryRecordBuilder.setMode(com.google.cloud.bigquery.Field.Mode.REPEATED);
  }

  private com.google.cloud.bigquery.Field.Builder convertPrimitive(Schema kafkaConnectSchema,
                                                                   String fieldName) {
    com.google.cloud.bigquery.Field.Type bigQueryType =
        PRIMITIVE_TYPE_MAP.get(kafkaConnectSchema.type());
    return com.google.cloud.bigquery.Field.newBuilder(fieldName, bigQueryType);
  }

  private com.google.cloud.bigquery.Field.Builder convertLogical(Schema kafkaConnectSchema,
                                                                 String fieldName) {
    LogicalTypeConverter converter =
        LogicalConverterRegistry.getConverter(kafkaConnectSchema.name());
    converter.checkEncodingType(kafkaConnectSchema.type());
    return com.google.cloud.bigquery.Field.newBuilder(fieldName, converter.getBQSchemaType());
  }
}
