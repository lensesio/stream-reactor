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

import com.wepay.kafka.connect.bigquery.SinkConnectorPropertiesFactory;

import org.apache.kafka.common.config.ConfigException;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class BigQuerySinkConnectorConfigTest {
  private SinkConnectorPropertiesFactory propertiesFactory;

  @Before
  public void initializePropertiesFactory() {
    propertiesFactory = new SinkConnectorPropertiesFactory();
  }

  @Test
  public void metaTestBasicConfigProperties() {
    Map<String, String> basicConfigProperties = propertiesFactory.getProperties();
    BigQuerySinkConnectorConfig config = new BigQuerySinkConnectorConfig(basicConfigProperties);
    propertiesFactory.testProperties(config);
  }

  @Test(expected = ConfigException.class)
  public void testAutoTableCreateWithoutRetriever() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();
    badConfigProperties.remove(BigQuerySinkConnectorConfig.SCHEMA_RETRIEVER_CONFIG);
    badConfigProperties.put(BigQuerySinkConnectorConfig.TABLE_CREATE_CONFIG, "true");

    new BigQuerySinkConnectorConfig(badConfigProperties);
  }
}
