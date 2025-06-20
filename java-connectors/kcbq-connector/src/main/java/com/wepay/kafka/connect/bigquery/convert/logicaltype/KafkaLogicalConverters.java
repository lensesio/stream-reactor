/*
 * Copyright 2017-2020 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.math.BigDecimal;

/**
 * Class containing all the Kafka logical type converters.
 */
public class KafkaLogicalConverters {

  static {
    LogicalConverterRegistry.register(Date.LOGICAL_NAME, new DateConverter());
    LogicalConverterRegistry.register(Decimal.LOGICAL_NAME, new DecimalConverter());
    LogicalConverterRegistry.register(Timestamp.LOGICAL_NAME, new TimestampConverter());
    LogicalConverterRegistry.register(Time.LOGICAL_NAME, new TimeConverter());
  }

  /**
   * Class for converting Kafka date logical types to Bigquery dates.
   */
  public static class DateConverter extends LogicalTypeConverter {

    /**
     * Create a new DateConverter.
     */
    public DateConverter() {
      super(Date.LOGICAL_NAME,
          Schema.Type.INT32,
          LegacySQLTypeName.DATE);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      return getBQDateFormat().format((java.util.Date) kafkaConnectObject);
    }
  }

  /**
   * Class for converting Kafka decimal logical types to Bigquery floating points.
   */
  public static class DecimalConverter extends LogicalTypeConverter {

    /**
     * Create a new DecimalConverter.
     */
    public DecimalConverter() {
      super(Decimal.LOGICAL_NAME,
          Schema.Type.BYTES,
          LegacySQLTypeName.FLOAT);
    }

    @Override
    public BigDecimal convert(Object kafkaConnectObject) {
      // cast to get ClassCastException
      return (BigDecimal) kafkaConnectObject;
    }
  }

  /**
   * Class for converting Kafka timestamp logical types to BigQuery timestamps.
   */
  public static class TimestampConverter extends LogicalTypeConverter {

    /**
     * Create a new TimestampConverter.
     */
    public TimestampConverter() {
      super(Timestamp.LOGICAL_NAME,
          Schema.Type.INT64,
          LegacySQLTypeName.TIMESTAMP);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      return getBqTimestampFormat().format((java.util.Date) kafkaConnectObject);
    }
  }

  /**
   * Class for converting Kafka time logical types to BigQuery time types.
   */
  public static class TimeConverter extends LogicalTypeConverter {

    /**
     * Create a new TimestampConverter.
     */
    public TimeConverter() {
      super(Time.LOGICAL_NAME,
          Schema.Type.INT32,
          LegacySQLTypeName.TIME);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      return getBqTimeFormat().format((java.util.Date) kafkaConnectObject);
    }
  }
}
