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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.cloud.bigquery.Field;

import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.DateConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.MicroTimeConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.MicroTimestampConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.TimeConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.TimestampConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.ZonedTimestampConverter;

import org.apache.kafka.connect.data.Schema;

import org.junit.Test;

public class DebeziumLogicalConvertersTest {

  //corresponds to March 1 2017, 22:20:38.808(123) UTC
  //              (March 1 2017, 14:20:38.808(123)-8:00)
  private static final Integer DAYS_TIMESTAMP = 17226;
  private static final Long MILLI_TIMESTAMP = 1488406838808L;
  private static final Long MICRO_TIMESTAMP = 1488406838808123L;

  @Test
  public void testDateConversion() {
    DateConverter converter = new DateConverter();

    assertEquals(Field.Type.date(), converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.INT32);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }
    
    String formattedDate = converter.convert(DAYS_TIMESTAMP);
    assertEquals("2017-03-01", formattedDate);
  }

  @Test
  public void testMicroTimeConversion() {
    MicroTimeConverter converter = new MicroTimeConverter();

    assertEquals(Field.Type.time(), converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.INT64);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    String formattedMicroTime = converter.convert(MICRO_TIMESTAMP);
    assertEquals("22:20:38.808123", formattedMicroTime);
  }

  @Test
  public void testMicroTimestampConversion() {
    MicroTimestampConverter converter = new MicroTimestampConverter();

    assertEquals(Field.Type.timestamp(), converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.INT64);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    String formattedMicroTimestamp = converter.convert(MICRO_TIMESTAMP);
    assertEquals("2017-03-01 22:20:38.808123", formattedMicroTimestamp);
  }

  @Test
  public void testTimeConversion() {
    TimeConverter converter = new TimeConverter();

    assertEquals(Field.Type.time(), converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.INT32);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    String formattedTime = converter.convert(MILLI_TIMESTAMP);
    assertEquals("22:20:38.808", formattedTime);
  }

  @Test
  public void testTimestampConversion() {
    TimestampConverter converter = new TimestampConverter();

    assertEquals(Field.Type.timestamp(), converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.INT64);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    String formattedTimestamp = converter.convert(MILLI_TIMESTAMP);
    assertEquals("2017-03-01 22:20:38.808", formattedTimestamp);
  }

  @Test
  public void testZonedTimestampConversion() {
    ZonedTimestampConverter converter = new ZonedTimestampConverter();

    assertEquals(Field.Type.timestamp(), converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.STRING);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    String formattedTimestamp = converter.convert("2017-03-01T14:20:38.808-08:00");
    assertEquals("2017-03-01 14:20:38.808-08:00", formattedTimestamp);
  }
}
