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

import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.protobuf.ByteString;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.LogicalConverterRegistry;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.LogicalTypeConverter;
import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class for converting from {@link SinkRecord SinkRecords} and BigQuery rows, which are represented
 * as {@link Map Maps} from {@link String Strings} to {@link Object Objects}.
 */
public class BigQueryRecordConverter implements RecordConverter<Map<String, Object>> {

  private static final Set<Class<?>> BASIC_TYPES = new HashSet<>(
          Arrays.asList(
            Boolean.class, Character.class, Byte.class, Short.class,
                  Integer.class, Long.class, Float.class, Double.class, String.class)
          );
  private final boolean shouldConvertSpecialDouble;
  private boolean shouldConvertDebeziumTimestampToInteger;
  private boolean useStorageWriteApi;

  static {
    // force registration
    new DebeziumLogicalConverters();
    new KafkaLogicalConverters();
  }

  public BigQueryRecordConverter(boolean shouldConvertDoubleSpecial,
                                 boolean shouldConvertDebeziumTimestampToInteger,
                                 boolean useStorageWriteApi) {
    this.shouldConvertSpecialDouble = shouldConvertDoubleSpecial;
    this.shouldConvertDebeziumTimestampToInteger = shouldConvertDebeziumTimestampToInteger;
    this.useStorageWriteApi = useStorageWriteApi;
  }

  /**
   * Convert a {@link SinkRecord} into the contents of a BigQuery {@link RowToInsert}.
   * @param record The Kafka Connect record to convert. Must be of type {@link Struct},
   *               in order to translate into a row format that requires each field to
   *               consist of both a name and a value.
   * @param recordType The type of the record to convert, either value or key.
   * @return The result BigQuery row content.
   */
  @SuppressWarnings("unchecked")
  public Map<String, Object> convertRecord(SinkRecord record, KafkaSchemaRecordType recordType) {
    Schema kafkaConnectSchema = recordType == KafkaSchemaRecordType.KEY ? record.keySchema() : record.valueSchema();
    Object kafkaConnectStruct = recordType == KafkaSchemaRecordType.KEY ? record.key() : record.value();
    if (kafkaConnectSchema == null) {
      if (kafkaConnectStruct instanceof Map) {
        return (Map<String, Object>) convertSchemalessRecord(kafkaConnectStruct);
      }
      throw new ConversionConnectException("Only Map objects supported in absence of schema for " +
              "record conversion to BigQuery format.");
    }
    if (kafkaConnectSchema.type() != Schema.Type.STRUCT) {
      throw new
          ConversionConnectException("Top-level Kafka Connect schema must be of type 'struct'");
    }
    return convertStruct(kafkaConnectStruct, kafkaConnectSchema);
  }

  @SuppressWarnings("unchecked")
  private Object convertSchemalessRecord(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Double) {
      return convertDouble((Double) value);
    }
    if (BASIC_TYPES.contains(value.getClass())) {
      return value;
    }
    if (value instanceof byte[] || value instanceof ByteBuffer) {
      return convertBytes(value);
    }
    if (value instanceof List) {
      return ((List<?>) value).stream()
          .map(this::convertSchemalessRecord)
          .collect(Collectors.toList());
    }
    if (value instanceof Map) {
      return
          ((Map<Object, Object>) value)
              .entrySet()
              .stream()
              .collect(HashMap::new,
                  (m, e) -> {
                    if (!(e.getKey() instanceof String)) {
                      throw new ConversionConnectException(
                          "Failed to convert record to bigQuery format: " +
                              "Map objects in absence of schema needs to have string value keys. ");
                    }
                    m.put(e.getKey(), convertSchemalessRecord(e.getValue()));
                  },
                  HashMap::putAll);
    }
    throw new ConversionConnectException("Unsupported class " + value.getClass() +
        " found in schemaless record data. Can't convert record to bigQuery format");
  }

  private Object convertObject(Object kafkaConnectObject, Schema kafkaConnectSchema) {
    if (kafkaConnectObject == null) {
      if (kafkaConnectSchema.isOptional()) {
        // short circuit converting the object
        return null;
      } else {
        throw new ConversionConnectException(
            kafkaConnectSchema.name() + " is not optional, but converting object had null value");
      }
    }
    if (LogicalConverterRegistry.isRegisteredLogicalType(kafkaConnectSchema.name())) {
      return convertLogical(kafkaConnectObject, kafkaConnectSchema);
    }
    Schema.Type kafkaConnectSchemaType = kafkaConnectSchema.type();
    switch (kafkaConnectSchemaType) {
      case ARRAY:
        return convertArray(kafkaConnectObject, kafkaConnectSchema);
      case MAP:
        return convertMap(kafkaConnectObject, kafkaConnectSchema);
      case STRUCT:
        return convertStruct(kafkaConnectObject, kafkaConnectSchema);
      case BYTES:
        return convertBytes(kafkaConnectObject);
      case FLOAT64:
        return convertDouble((Double)kafkaConnectObject);
      case BOOLEAN:
      case FLOAT32:
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case STRING:
        return kafkaConnectObject;
      default:
        throw new ConversionConnectException("Unrecognized schema type: " + kafkaConnectSchemaType);
    }
  }

  private Map<String, Object> convertStruct(Object kafkaConnectObject, Schema kafkaConnectSchema) {
    Map<String, Object> bigQueryRecord = new HashMap<>();
    List<Field> kafkaConnectSchemaFields = kafkaConnectSchema.fields();
    Struct kafkaConnectStruct = (Struct) kafkaConnectObject;
    for (Field kafkaConnectField : kafkaConnectSchemaFields) {
      // ignore empty structures
      boolean isEmptyStruct = kafkaConnectField.schema().type() == Schema.Type.STRUCT &&
          kafkaConnectField.schema().fields().isEmpty();
      if (!isEmptyStruct) {
        Object bigQueryObject = convertObject(
            kafkaConnectStruct.get(kafkaConnectField.name()),
            kafkaConnectField.schema()
        );
        if (bigQueryObject != null) {
          bigQueryRecord.put(kafkaConnectField.name(), bigQueryObject);
        }
      }
    }
    return bigQueryRecord;
  }

  @SuppressWarnings("unchecked")
  private List<Object> convertArray(Object kafkaConnectObject,
                                    Schema kafkaConnectSchema) {
    Schema kafkaConnectValueSchema = kafkaConnectSchema.valueSchema();
    List<Object> bigQueryList = new ArrayList<>();
    List<Object> kafkaConnectList = (List<Object>) kafkaConnectObject;
    for (Object kafkaConnectElement : kafkaConnectList) {
      Object bigQueryValue = convertObject(kafkaConnectElement, kafkaConnectValueSchema);
      bigQueryList.add(bigQueryValue);
    }
    return bigQueryList;
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> convertMap(Object kafkaConnectObject,
                                               Schema kafkaConnectSchema) {
    Schema kafkaConnectKeySchema = kafkaConnectSchema.keySchema();
    Schema kafkaConnectValueSchema = kafkaConnectSchema.valueSchema();
    List<Map<String, Object>> bigQueryEntryList = new ArrayList<>();
    Map<Object, Object> kafkaConnectMap = (Map<Object, Object>) kafkaConnectObject;
    for (Map.Entry<Object, Object> kafkaConnectMapEntry : kafkaConnectMap.entrySet()) {
      Map<String, Object> bigQueryEntry = new HashMap<>();
      Object bigQueryKey = convertObject(
          kafkaConnectMapEntry.getKey(),
          kafkaConnectKeySchema
      );
      Object bigQueryValue = convertObject(
          kafkaConnectMapEntry.getValue(),
          kafkaConnectValueSchema
      );
      bigQueryEntry.put(BigQuerySchemaConverter.MAP_KEY_FIELD_NAME, bigQueryKey);
      bigQueryEntry.put(BigQuerySchemaConverter.MAP_VALUE_FIELD_NAME, bigQueryValue);
      bigQueryEntryList.add(bigQueryEntry);
    }
    return bigQueryEntryList;
  }

  private Object convertLogical(Object kafkaConnectObject,
                                Schema kafkaConnectSchema) {
    LogicalTypeConverter converter =
        LogicalConverterRegistry.getConverter(kafkaConnectSchema.name());

    if(shouldConvertDebeziumTimestampToInteger && converter instanceof DebeziumLogicalConverters.TimestampConverter) {
      return (Long) kafkaConnectObject;
    }
    return converter.convert(kafkaConnectObject);
  }

  /**
   * Converts a kafka connect {@link Double} into a value that can be stored into BigQuery
   * If this.shouldDonvertSpecialDouble is true, special values are converted as follows:
   * Double.POSITIVE_INFINITY -> Double.MAX_VALUE
   * Doulbe.NEGATIVE_INFINITY -> Double.MIN_VALUE
   * Double.NaN               -> Double.MIN_VALUE
   *
   * @param kafkaConnectDouble The Kafka Connect value to convert.
   *
   * @return The resulting Double value to put in BigQuery.
   */
  private Double convertDouble(Double kafkaConnectDouble) {
    if (shouldConvertSpecialDouble) {
      if (kafkaConnectDouble.equals(Double.POSITIVE_INFINITY)) {
        return Double.MAX_VALUE;
      } else if (kafkaConnectDouble.equals(Double.NEGATIVE_INFINITY)
              || Double.isNaN(kafkaConnectDouble)) {
        return Double.MIN_VALUE;
      }
    }
    return kafkaConnectDouble;
  }

  private Object convertBytes(Object kafkaConnectObject) {
    byte[] bytes;
    if (kafkaConnectObject instanceof ByteBuffer) {
      ByteBuffer byteBuffer = (ByteBuffer) kafkaConnectObject;
      bytes = byteBuffer.array();
    } else {
      bytes = (byte[]) kafkaConnectObject;
    }
    return useStorageWriteApi? ByteString.copyFrom(bytes) : Base64.getEncoder().encodeToString(bytes);
  }
}
