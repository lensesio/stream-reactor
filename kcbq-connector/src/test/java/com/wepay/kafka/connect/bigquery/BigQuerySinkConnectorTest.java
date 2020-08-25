package com.wepay.kafka.connect.bigquery;

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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import static org.mockito.Matchers.any;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.SinkConfigConnectException;
import org.apache.kafka.common.config.ConfigException;

import org.apache.kafka.connect.data.Schema;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigQuerySinkConnectorTest {
  private static SinkConnectorPropertiesFactory propertiesFactory;

  // Would just use Mockito, but can't provide the name of an anonymous class to the config file
  public static class MockSchemaRetriever implements SchemaRetriever {
    @Override
    public void configure(Map<String, String> properties) {
      // Shouldn't be called
    }

    @Override
    public Schema retrieveKeySchema(SinkRecord record){
      return null;
    }

    @Override
    public Schema retrieveValueSchema(SinkRecord record){
      return null;
    }
  }

  @BeforeClass
  public static void initializePropertiesFactory() {
    propertiesFactory = new SinkConnectorPropertiesFactory();
  }

  @Test
  public void testTaskClass() {
    assertEquals(BigQuerySinkTask.class, new BigQuerySinkConnector().taskClass());
  }

  @Test
  public void testTaskConfigs() {
    Map<String, String> properties = propertiesFactory.getProperties();

    Table fakeTable = mock(Table.class);

    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.getTable(any(TableId.class))).thenReturn(fakeTable);

    SchemaManager schemaManager = mock(SchemaManager.class);
    BigQuerySinkConnector testConnector = new BigQuerySinkConnector(bigQuery, schemaManager);

    testConnector.start(properties);

    for (int i : new int[] { 1, 2, 10, 100 }) {
      Map<String, String> expectedProperties = new HashMap<>(properties);
      List<Map<String, String>> taskConfigs = testConnector.taskConfigs(i);
      assertEquals(i, taskConfigs.size());
      for (int j = 0; j < i; j++) {
        assertEquals(
            "Connector properties should match task configs",
            expectedProperties,
            taskConfigs.get(j)
        );
        assertNotSame(
            "Properties should not be referentially equal to task config",
            properties,
            taskConfigs.get(j)
        );
        // A little overboard, sure, but since it's only in the ballpark of 10,000 iterations this
        // should be fine
        for (int k = j + 1; k < i; k++) {
          assertNotSame(
              "Task configs should not be referentially equal to each other",
              taskConfigs.get(j),
              taskConfigs.get(k)
          );
        }
      }
    }
  }

  @Test
  public void testConfig() {
    assertEquals(BigQuerySinkConfig.getConfig(), new BigQuerySinkConnector().config());
  }

  // Make sure that a config exception is properly translated into a SinkConfigConnectException
  @Test(expected = SinkConfigConnectException.class)
  public void testConfigException() {
    try {
      Map<String, String> badProperties = propertiesFactory.getProperties();
      badProperties.remove(BigQuerySinkConfig.TOPICS_CONFIG);
      BigQuerySinkConfig.validate(badProperties);
      new BigQuerySinkConnector().start(badProperties);
    } catch (ConfigException e) {
      throw new SinkConfigConnectException(e);
    }
  }

  @Test
  public void testVersion() {
    assertNotNull(new BigQuerySinkConnector().version());
  }

  // Doesn't do anything at the moment, but having this here will encourage tests to be written if
  // the stop() method ever does anything significant
  @Test
  public void testStop() {
    new BigQuerySinkConnector().stop();
  }
}
