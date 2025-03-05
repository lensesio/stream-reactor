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

package com.wepay.kafka.connect.bigquery.convert.logicaltype;

import com.google.cloud.bigquery.LegacySQLTypeName;

import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;

import org.apache.kafka.connect.data.Schema;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Abstract class for logical type converters.
 * Contains logic for both schema and record conversions.
 */
public abstract class LogicalTypeConverter {

  // BigQuery uses UTC timezone by default
  protected static final TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");

  private String logicalName;
  private Schema.Type encodingType;
  private LegacySQLTypeName bqSchemaType;

  /**
   * Create a new LogicalConverter.
   *
   * @param logicalName The name of the logical type.
   * @param encodingType The encoding type of the logical type.
   * @param bqSchemaType The corresponding BigQuery Schema type of the logical type.
   */
  public LogicalTypeConverter(String logicalName,
                              Schema.Type encodingType,
                              LegacySQLTypeName bqSchemaType) {
    this.logicalName = logicalName;
    this.encodingType = encodingType;
    this.bqSchemaType = bqSchemaType;
  }

  /**
   * @param encodingType the encoding type to check.
   * @throws ConversionConnectException if the given schema encoding type is not the same as the
   *                                    expected encoding type.
   */
  public void checkEncodingType(Schema.Type encodingType) throws ConversionConnectException {
    if (encodingType != this.encodingType) {
      throw new ConversionConnectException(
        "Logical Type " + logicalName + " must be encoded as " + this.encodingType + "; "
          + "instead, found " + encodingType
      );
    }
  }

  public LegacySQLTypeName getBQSchemaType() {
    return bqSchemaType;
  }

  /**
   * Convert the given KafkaConnect Record Object to a BigQuery Record Object.
   *
   * @param kafkaConnectObject the kafkaConnectObject
   * @return the converted Object
   */
  public abstract Object convert(Object kafkaConnectObject);

  protected static SimpleDateFormat getBqTimestampFormat() {
    SimpleDateFormat bqTimestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    bqTimestampFormat.setTimeZone(utcTimeZone);
    return bqTimestampFormat;
  }

  protected SimpleDateFormat getBqTimeFormat() {
    SimpleDateFormat bqTimestampFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    bqTimestampFormat.setTimeZone(utcTimeZone);
    return bqTimestampFormat;
  }

  protected static SimpleDateFormat getBQDateFormat() {
    SimpleDateFormat bqDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    bqDateFormat.setTimeZone(utcTimeZone);
    return bqDateFormat;
  }

  protected static SimpleDateFormat getBQTimeFormat() {
    SimpleDateFormat bqTimeFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    bqTimeFormat.setTimeZone(utcTimeZone);
    return bqTimeFormat;
  }

}
