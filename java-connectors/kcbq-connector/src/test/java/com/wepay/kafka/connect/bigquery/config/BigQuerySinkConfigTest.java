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

import com.google.cloud.bigquery.TimePartitioning;
import com.wepay.kafka.connect.bigquery.SinkPropertiesFactory;
import com.wepay.kafka.connect.bigquery.convert.BigQueryRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.BigQuerySchemaConverter;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.CONNECTOR_RUNTIME_PROVIDER_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.CONNECTOR_RUNTIME_PROVIDER_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    config.getList(BigQuerySinkConfig.TOPICS_CONFIG);
    config.getString(BigQuerySinkConfig.PROJECT_CONFIG);
    config.getKey();
    config.getBoolean(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG);
    config.getInt(BigQuerySinkConfig.AVRO_DATA_CACHE_SIZE_CONFIG);
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

  /**
   * Test the default for the field name is not present.
   */
  @Test
  public void testEmptyTimestampPartitionFieldName() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    assertFalse(testConfig.getTimestampPartitionFieldName().isPresent());
  }

  /**
   * Test the field name being non-empty and the decorator set to false works correctly.
   */
  @Test
  public void testTimestampPartitionFieldName() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_CONFIG, "name");
    configProperties.put(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "false");
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    assertTrue(testConfig.getTimestampPartitionFieldName().isPresent());
    assertFalse(testConfig.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG));
  }

  /**
   * Test the default for the field names is not present.
   */
  @Test
  public void testEmptyClusteringFieldNames() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    assertFalse(testConfig.getClusteringPartitionFieldNames().isPresent());
  }

  /**
   * Test if the field names are more than four fields errors correctly.
   */
  @Test (expected = ConfigException.class)
  public void testClusteringPartitionFieldNamesWithMoreThanFourFieldsError() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.BIGQUERY_CLUSTERING_FIELD_NAMES_CONFIG,
        "column1,column2,column3,column4,column5"
    );
    new BigQuerySinkConfig(configProperties);
  }

  /**
   * Test the field names being non-empty and the partitioning field exists works correctly.
   */
  @Test
  public void testClusteringFieldNames() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_CONFIG, "name");
    configProperties.put(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "false");
    configProperties.put(
        BigQuerySinkConfig.BIGQUERY_CLUSTERING_FIELD_NAMES_CONFIG,
        "column1,column2"
    );

    ArrayList<String> expectedClusteringPartitionFieldName = new ArrayList<>(
        Arrays.asList("column1", "column2")
    );

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    Optional<List<String>> testClusteringPartitionFieldName = testConfig.getClusteringPartitionFieldNames();
    assertTrue(testClusteringPartitionFieldName.isPresent());
    assertEquals(expectedClusteringPartitionFieldName, testClusteringPartitionFieldName.get());
  }

  /**
   * Test the default for the partition expiration is not present.
   */
  @Test
  public void testEmptyPartitionExpirationMs() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    assertFalse(testConfig.getPartitionExpirationMs().isPresent());
  }

  /**
   * Test the partition expiration is set correctly for a valid value.
   */
  @Test
  public void testValidPartitionExpirationMs() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.BIGQUERY_PARTITION_EXPIRATION_CONFIG, "1");
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    assertTrue(testConfig.getPartitionExpirationMs().isPresent());
    assertEquals(Optional.of(1L), testConfig.getPartitionExpirationMs());
  }

  /**
   * Test the partition expiration being non-positive errors correctly.
   */
  @Test (expected = ConfigException.class)
  public void testMinimumPartitionExpirationMs() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.BIGQUERY_PARTITION_EXPIRATION_CONFIG, "0");
    new BigQuerySinkConfig(configProperties);
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
    assertEquals(Optional.empty(), timePartitioningType);
  }

  @Test(expected = ConfigException.class)
  public void testInvalidTimePartitioningType() {
    Map<String, String> configProperties = propertiesFactory.getProperties();

    configProperties.put(BigQuerySinkConfig.TIME_PARTITIONING_TYPE_CONFIG, "fortnight");
    new BigQuerySinkConfig(configProperties);
  }

  @Test
  public void testKafkaProviderConfigDefaultValue() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    assertEquals(CONNECTOR_RUNTIME_PROVIDER_DEFAULT, config.getString(CONNECTOR_RUNTIME_PROVIDER_CONFIG));
  }

  @Test
  public void testKafkaProviderConfig() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    String testKafkaProvider = "testProvider";
    configProperties.put(CONNECTOR_RUNTIME_PROVIDER_CONFIG, testKafkaProvider);
    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    assertEquals(testKafkaProvider, config.getString(CONNECTOR_RUNTIME_PROVIDER_CONFIG));
  }

  @Test (expected = ConfigException.class)
  public void testTopic2TableInvalidFormat() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.TOPIC2TABLE_MAP_CONFIG, "topic:");
    new BigQuerySinkConfig(configProperties);
  }

  @Test (expected = ConfigException.class)
  public void testTopic2TableDuplicateTopic() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.TOPIC2TABLE_MAP_CONFIG, "topic:table, topic:table2");
    new BigQuerySinkConfig(configProperties);
  }

  @Test
  public void testTopic2TableDuplicateTable() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.TOPIC2TABLE_MAP_CONFIG, "topic:table, topic2:table");
    new BigQuerySinkConfig(configProperties);
  }

  @Test (expected = ConfigException.class)
  public void testTopic2TableSemicolonOnly() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.TOPIC2TABLE_MAP_CONFIG, ":");
    new BigQuerySinkConfig(configProperties);
  }

  @Test
  public void testValidTopic2TableMap() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.TOPIC2TABLE_MAP_CONFIG, "topic:table, topic2:table2");
    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    Map<String, String> topic2TableMap = new HashMap<>();
    topic2TableMap.put("topic", "table");
    topic2TableMap.put("topic2", "table2");
    assertEquals(topic2TableMap, config.getTopic2TableMap().get());
  }

  @Test
  public void testTopic2TableEmptyString() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.TOPIC2TABLE_MAP_CONFIG, "");
    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    assertFalse(config.getTopic2TableMap().isPresent());
  }

  @Test
  public void testTopic2TableCommaOnly() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.TOPIC2TABLE_MAP_CONFIG, ",");
    BigQuerySinkConfig config = new BigQuerySinkConfig(configProperties);
    assertFalse(config.getTopic2TableMap().isPresent());
  }

  @Test(expected = ConfigException.class)
  public void testInvalidMaxRetries() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();

    badConfigProperties.put(
            BigQuerySinkConfig.MAX_RETRIES_CONFIG,
            "-1"
    );

    new BigQuerySinkConfig(badConfigProperties);
  }
}
