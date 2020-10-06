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

import com.wepay.kafka.connect.bigquery.SinkTaskPropertiesFactory;

import org.apache.kafka.common.config.ConfigException;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BigQuerySinkTaskConfigTest {
  private SinkTaskPropertiesFactory propertiesFactory;

  @Before
  public void initializePropertiesFactory() {
    propertiesFactory = new SinkTaskPropertiesFactory();
  }

  @Test
  public void metaTestBasicConfigProperties() {
    Map<String, String> basicConfigProperties = propertiesFactory.getProperties();
    BigQuerySinkTaskConfig config = new BigQuerySinkTaskConfig(basicConfigProperties);
    propertiesFactory.testProperties(config);
  }

  @Test()
  public void testMaxWriteSize() {
    // todo: something like this, maybe.
    /*
    Map<String, String> badProperties = propertiesFactory.getProperties();
    badProperties.put(BigQuerySinkTaskConfig.MAX_WRITE_CONFIG, "-1");

    try {
      new BigQuerySinkTaskConfig(badProperties);
    } catch (ConfigException err) {
      fail("Exception encountered before addition of bad configuration field: " + err);
    }

    badProperties.put(BigQuerySinkTaskConfig.MAX_WRITE_CONFIG, "0");
    new BigQuerySinkTaskConfig(badProperties);
    */
  }

  /**
   * Test the default for the field name is not present.
   */
  @Test
  public void testEmptyTimestampPartitionFieldName() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    BigQuerySinkTaskConfig testConfig = new BigQuerySinkTaskConfig(configProperties);
    assertFalse(testConfig.getTimestampPartitionFieldName().isPresent());
  }

  /**
   * Test if the field name being non-empty and the decorator default (true) errors correctly.
   */
  @Test (expected = ConfigException.class)
  public void testTimestampPartitionFieldNameError() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkTaskConfig.BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_CONFIG, "name");
    new BigQuerySinkTaskConfig(configProperties);
  }

  /**
   * Test the field name being non-empty and the decorator set to false works correctly.
   */
  @Test
  public void testTimestampPartitionFieldName() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkTaskConfig.BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_CONFIG, "name");
    configProperties.put(BigQuerySinkTaskConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "false");
    BigQuerySinkTaskConfig testConfig = new BigQuerySinkTaskConfig(configProperties);
    assertTrue(testConfig.getTimestampPartitionFieldName().isPresent());
    assertFalse(testConfig.getBoolean(BigQuerySinkTaskConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG));
  }

  /**
   * Test the default for the field names is not present.
   */
  @Test
  public void testEmptyClusteringFieldNames() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    BigQuerySinkTaskConfig testConfig = new BigQuerySinkTaskConfig(configProperties);
    assertFalse(testConfig.getClusteringPartitionFieldName().isPresent());
  }

  /**
   * Test if the field names being non-empty and the partitioning is not present errors correctly.
   */
  @Test (expected = ConfigException.class)
  public void testClusteringFieldNamesWithoutTimestampPartitionError() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkTaskConfig.BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_CONFIG, null);
    configProperties.put(BigQuerySinkTaskConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "false");
    configProperties.put(
        BigQuerySinkTaskConfig.BIGQUERY_CLUSTERING_FIELD_NAMES_CONFIG,
        "column1,column2"
    );
    new BigQuerySinkTaskConfig(configProperties);
  }

  /**
   * Test if the field names are more than four fields errors correctly.
   */
  @Test (expected = ConfigException.class)
  public void testClusteringPartitionFieldNamesWithMoreThanFourFieldsError() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkTaskConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "true");
    configProperties.put(
        BigQuerySinkTaskConfig.BIGQUERY_CLUSTERING_FIELD_NAMES_CONFIG,
        "column1,column2,column3,column4,column5"
    );
    new BigQuerySinkTaskConfig(configProperties);
  }

  /**
   * Test the field names being non-empty and the partitioning field exists works correctly.
   */
  @Test
  public void testClusteringFieldNames() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkTaskConfig.BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_CONFIG, "name");
    configProperties.put(BigQuerySinkTaskConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "false");
    configProperties.put(
        BigQuerySinkTaskConfig.BIGQUERY_CLUSTERING_FIELD_NAMES_CONFIG,
        "column1,column2"
    );

    ArrayList<String> expectedClusteringPartitionFieldName = new ArrayList<>(
        Arrays.asList("column1", "column2")
    );

    BigQuerySinkTaskConfig testConfig = new BigQuerySinkTaskConfig(configProperties);
    Optional<List<String>> testClusteringPartitionFieldName = testConfig.getClusteringPartitionFieldName();
    assertTrue(testClusteringPartitionFieldName.isPresent());
    assertEquals(expectedClusteringPartitionFieldName, testClusteringPartitionFieldName.get());
  }

  @Test(expected = ConfigException.class)
  public void testSchemaUpdatesWithoutRetriever() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();
    badConfigProperties.remove(BigQuerySinkTaskConfig.SCHEMA_RETRIEVER_CONFIG);
    badConfigProperties.put(BigQuerySinkTaskConfig.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG, "true");
    badConfigProperties.put(BigQuerySinkTaskConfig.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG, "true");

    new BigQuerySinkTaskConfig(badConfigProperties);
  }
}
