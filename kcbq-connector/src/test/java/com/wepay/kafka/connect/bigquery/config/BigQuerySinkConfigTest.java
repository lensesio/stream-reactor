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

package com.wepay.kafka.connect.bigquery.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.TimePartitioning;
import com.wepay.kafka.connect.bigquery.SinkPropertiesFactory;

import com.wepay.kafka.connect.bigquery.convert.BigQueryRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.BigQuerySchemaConverter;
import org.apache.kafka.common.config.ConfigException;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

public class BigQuerySinkConfigTest {
  private SinkPropertiesFactory propertiesFactory;

  @Before
  public void initializePropertiesFactory() {
    propertiesFactory = new SinkPropertiesFactory();
  }

  // Just to ensure that the basic properties don't cause any exceptions on any public methods
  @Test
  public void metaTestBasicConfigProperties() {
    Map<String, String> basicConfigProperties = propertiesFactory.getProperties();
    BigQuerySinkConfig config = new BigQuerySinkConfig(basicConfigProperties);
    propertiesFactory.testProperties(config);
  }

  @Test
  public void testGetSchemaConverter() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaData");

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);

    assertTrue(testConfig.getSchemaConverter() instanceof BigQuerySchemaConverter);
  }

  @Test
  public void testGetRecordConverter() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaData");

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);

    assertTrue(testConfig.getRecordConverter() instanceof BigQueryRecordConverter);
  }

  @Test(expected = ConfigException.class)
  public void testInvalidAvroCacheSize() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();

    badConfigProperties.put(
        BigQuerySinkConfig.AVRO_DATA_CACHE_SIZE_CONFIG,
        "-1"
    );

    new BigQuerySinkConfig(badConfigProperties);
  }

  @Test
  public void testValidTimePartitioningTypes() {
    Map<String, String> configProperties = propertiesFactory.getProperties();

    for (TimePartitioning.Type type : TimePartitioning.Type.values()) {
      configProperties.put(BigQuerySinkConfig.TIME_PARTITIONING_TYPE_CONFIG, type.name());
      Optional<TimePartitioning.Type> timePartitioningType = new BigQuerySinkConfig(configProperties).getTimePartitioningType();
      assertTrue(timePartitioningType.isPresent());
      assertEquals(type, timePartitioningType.get());
    }

    configProperties.put(BigQuerySinkConfig.TIME_PARTITIONING_TYPE_CONFIG, BigQuerySinkConfig.TIME_PARTITIONING_TYPE_NONE);
    Optional<TimePartitioning.Type> timePartitioningType = new BigQuerySinkConfig(configProperties).getTimePartitioningType();
    assertFalse(timePartitioningType.isPresent());
  }

  @Test(expected = ConfigException.class)
  public void testInvalidTimePartitioningType() {
    Map<String, String> configProperties = propertiesFactory.getProperties();

    configProperties.put(BigQuerySinkConfig.TIME_PARTITIONING_TYPE_CONFIG, "fortnight");
    new BigQuerySinkConfig(configProperties);
  }
}
