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

import com.google.cloud.bigquery.LegacySQLTypeName;

import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters.DateConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters.DecimalConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters.TimestampConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters.TimeConverter;

import org.apache.kafka.connect.data.Schema;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.Date;

public class KafkaLogicalConvertersTest {

  //corresponds to March 1 2017, 22:20:38.808
  private static final Long TIMESTAMP = 1488406838808L;

  @Test
  public void testDateConversion() {
    DateConverter converter = new DateConverter();

    assertEquals(LegacySQLTypeName.DATE, converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.INT32);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    Date date = new Date(TIMESTAMP);
    String formattedDate = converter.convert(date);
    assertEquals("2017-03-01", formattedDate);
  }

  @Test
  public void testDecimalConversion() {
    DecimalConverter converter = new DecimalConverter();

    assertEquals(LegacySQLTypeName.FLOAT, converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.BYTES);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    BigDecimal bigDecimal = new BigDecimal("3.14159");

    BigDecimal convertedDecimal = converter.convert(bigDecimal);

    // expecting no-op
    assertEquals(bigDecimal, convertedDecimal);
  }

  @Test
  public void testTimestampConversion() {
    TimestampConverter converter = new TimestampConverter();

    assertEquals(LegacySQLTypeName.TIMESTAMP, converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.INT64);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    try {
      converter.checkEncodingType(Schema.Type.INT32);
      fail("Expected encoding type check to fail");
    } catch (Exception ex) {
      // continue
    }

    Date date = new Date(TIMESTAMP);
    String formattedTimestamp = converter.convert(date);

    assertEquals("2017-03-01 22:20:38.808", formattedTimestamp);
  }


  @Test
  public void testTimeConversion() {
    TimeConverter converter = new KafkaLogicalConverters.TimeConverter();

    assertEquals(LegacySQLTypeName.TIME, converter.getBQSchemaType());

    try {
      converter.checkEncodingType(Schema.Type.INT32);
    } catch (Exception ex) {
      fail("Expected encoding type check to succeed.");
    }

    try {
      converter.checkEncodingType(Schema.Type.INT64);
      fail("Expected encoding type check to fail");
    } catch (Exception ex) {
      // continue
    }

    // Can't use the same timestamp here as the one in other tests as the Time type
    // should only fall on January 1st, 1970
    Date date = new Date(166838808);
    String formattedTimestamp = converter.convert(date);

    assertEquals("22:20:38.808", formattedTimestamp);
  }
}
