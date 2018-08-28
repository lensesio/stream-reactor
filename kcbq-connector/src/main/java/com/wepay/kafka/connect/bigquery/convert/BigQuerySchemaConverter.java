package com.wepay.kafka.connect.bigquery.convert;

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


import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;

import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.LogicalConverterRegistry;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.LogicalTypeConverter;
import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
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

  private static final Map<Schema.Type, LegacySQLTypeName> PRIMITIVE_TYPE_MAP;

  static {
    // force registration
    new DebeziumLogicalConverters();
    new KafkaLogicalConverters();

    PRIMITIVE_TYPE_MAP = new HashMap<>();
    PRIMITIVE_TYPE_MAP.put(Schema.Type.BOOLEAN,
                           LegacySQLTypeName.BOOLEAN);
    PRIMITIVE_TYPE_MAP.put(Schema.Type.FLOAT32,
                           LegacySQLTypeName.FLOAT);
    PRIMITIVE_TYPE_MAP.put(Schema.Type.FLOAT64,
                           LegacySQLTypeName.FLOAT);
    PRIMITIVE_TYPE_MAP.put(Schema.Type.INT8,
                           LegacySQLTypeName.INTEGER);
    PRIMITIVE_TYPE_MAP.put(Schema.Type.INT16,
                           LegacySQLTypeName.INTEGER);
    PRIMITIVE_TYPE_MAP.put(Schema.Type.INT32,
                           LegacySQLTypeName.INTEGER);
    PRIMITIVE_TYPE_MAP.put(Schema.Type.INT64,
                           LegacySQLTypeName.INTEGER);
    PRIMITIVE_TYPE_MAP.put(Schema.Type.STRING,
                           LegacySQLTypeName.STRING);
    PRIMITIVE_TYPE_MAP.put(Schema.Type.BYTES,
                           LegacySQLTypeName.BYTES);
  }

  private final boolean allFieldsNullable;

  public BigQuerySchemaConverter(boolean allFieldsNullable) {
    this.allFieldsNullable = allFieldsNullable;
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

    List<com.google.cloud.bigquery.Field> fields = new LinkedList<>();

    for (Field kafkaConnectField : kafkaConnectSchema.fields()) {
      com.google.cloud.bigquery.Field bigQuerySchemaField =
          convertField(kafkaConnectField.schema(), kafkaConnectField.name()).build();
      fields.add(bigQuerySchemaField);
    }

    return com.google.cloud.bigquery.Schema.of(fields);
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
        if (allFieldsNullable || kafkaConnectSchema.isOptional()) {
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

    FieldList fieldList = FieldList.of(bigQueryRecordFields);

    return com.google.cloud.bigquery.Field.newBuilder(fieldName,
                                                      LegacySQLTypeName.RECORD,
                                                      fieldList);
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

    com.google.cloud.bigquery.Field keyField =
        convertField(keySchema, MAP_KEY_FIELD_NAME).build();
    com.google.cloud.bigquery.Field valueField =
        convertField(valueSchema, MAP_VALUE_FIELD_NAME).build();

    com.google.cloud.bigquery.Field.Builder bigQueryRecordBuilder =
        com.google.cloud.bigquery.Field.newBuilder(fieldName,
                                                   LegacySQLTypeName.RECORD,
                                                   keyField,
                                                   valueField);

    return bigQueryRecordBuilder.setMode(com.google.cloud.bigquery.Field.Mode.REPEATED);
  }

  private com.google.cloud.bigquery.Field.Builder convertPrimitive(Schema kafkaConnectSchema,
                                                                   String fieldName) {
    LegacySQLTypeName bigQueryType =
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
