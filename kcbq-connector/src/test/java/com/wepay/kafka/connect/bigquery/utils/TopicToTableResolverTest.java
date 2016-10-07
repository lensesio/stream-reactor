package com.wepay.kafka.connect.bigquery.utils;

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

import com.wepay.kafka.connect.bigquery.SinkPropertiesFactory;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TopicToTableResolverTest {

  private SinkPropertiesFactory propertiesFactory;

  @Before
  public void initializePropertiesFactory() {
    propertiesFactory = new SinkPropertiesFactory();
  }

  @Test
  public void testGetTopicsToTablesMap() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "sanitize-me,db_debezium_identity_profiles_info,db.core.cluster-0.users"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "db_debezium_identity_profiles_(.*)=$1,db\\.(.*)\\.(.*)\\.(.*)=$1_$3"
    );
    Map<String, String> expectedTopicsToTables = new HashMap<>();
    expectedTopicsToTables.put("sanitize-me", "sanitize_me");
    expectedTopicsToTables.put("db_debezium_identity_profiles_info", "info");
    expectedTopicsToTables.put("db.core.cluster-0.users", "core_users");

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    Map<String, String> topicsToTables = TopicToTableResolver.getTopicsToTables(testConfig);

    assertEquals(expectedTopicsToTables, topicsToTables);
  }

  @Test(expected=ConfigException.class)
  public void testTopicsToTablesInvalidRegex() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "db_debezium_identity_profiles_info"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        ".*=$1"
    );
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.getTopicsToTables(testConfig);
  }

  @Test(expected=ConfigException.class)
  public void testTopicsToTablesMultipleMatches() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "db_debezium_identity_profiles_info"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "(.*)=$1,db_debezium_identity_profiles_(.*)=$1"
    );
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.getTopicsToTables(testConfig);
  }
}
