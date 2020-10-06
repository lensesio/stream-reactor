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

import static org.junit.Assert.fail;

import com.wepay.kafka.connect.bigquery.SinkTaskPropertiesFactory;

import org.apache.kafka.common.config.ConfigException;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

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

  @Test(expected = ConfigException.class)
  public void testAutoSchemaUpdateWithoutRetriever() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();
    badConfigProperties.remove(BigQuerySinkTaskConfig.SCHEMA_RETRIEVER_CONFIG);
    badConfigProperties.put(BigQuerySinkTaskConfig.SCHEMA_UPDATE_CONFIG, "true");

    new BigQuerySinkTaskConfig(badConfigProperties);
  }
}
