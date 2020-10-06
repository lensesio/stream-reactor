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

package com.wepay.kafka.connect.bigquery.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.SinkPropertiesFactory;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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
        "sanitize-me,db_debezium_identity_profiles_info.foo,db.core.cluster-0.users"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "db_debezium_identity_profiles_(.*)=$1,db\\.(.*)\\.(.*)\\.(.*)=$1_$3"
    );
    Map<String, TableId> expectedTopicsToTables = new HashMap<>();
    expectedTopicsToTables.put("sanitize-me", TableId.of("scratch", "sanitize_me"));
    expectedTopicsToTables.put("db_debezium_identity_profiles_info.foo",
        TableId.of("scratch", "info_foo"));
    expectedTopicsToTables.put("db.core.cluster-0.users", TableId.of("scratch", "core_users"));

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    Map<String, TableId> topicsToTableIds = TopicToTableResolver.getTopicsToTables(testConfig);

    assertEquals(expectedTopicsToTables, topicsToTableIds);
  }

  @Test(expected = ConfigException.class)
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

  @Test(expected = ConfigException.class)
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

  @Test
  public void testUpdateTopicToTable() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "sanitize-me,db_debezium_identity_profiles_info.foo,db.core.cluster-0.users"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "db_debezium_identity_profiles_(.*)=$1,db\\.(.*)\\.(.*)\\.(.*)=$1_$3"
    );
    Map<String, TableId> topicsToTables = new HashMap<>();
    topicsToTables.put("sanitize-me", TableId.of("scratch", "sanitize_me"));
    topicsToTables.put("db_debezium_identity_profiles_info.foo",
        TableId.of("scratch", "info_foo"));
    topicsToTables.put("db.core.cluster-0.users", TableId.of("scratch", "core_users"));

    String testTopicName = "new_topic";
    // Create shallow copy of map, deep copy not needed.
    Map<String, TableId> expectedTopicsToTables = new HashMap<>(topicsToTables);
    expectedTopicsToTables.put(testTopicName, TableId.of("scratch", testTopicName));

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.updateTopicToTable(testConfig, testTopicName, topicsToTables);

    assertEquals(expectedTopicsToTables, topicsToTables);
  }

  @Test
  public void testUpdateTopicToTableWithTableSanitization() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "db_debezium_identity_profiles_(.*)=$1,db\\.(.*)\\.(.*)\\.(.*)=$1_$3"
    );

    String testTopicName = "1new.topic";
    Map<String, TableId> topicsToTables = new HashMap<>();
    // Create shallow copy of map, deep copy not needed.
    Map<String, TableId> expectedTopicsToTables = new HashMap<>(topicsToTables);
    expectedTopicsToTables.put(testTopicName, TableId.of("scratch", "_1new_topic"));

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.updateTopicToTable(testConfig, testTopicName, topicsToTables);

    assertEquals(expectedTopicsToTables, topicsToTables);
  }

  @Test
  public void testUpdateTopicToTableWithRegex() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "db_debezium_identity_profiles_(.*)=$1,db\\.(.*)\\.(.*)\\.(.*)=$1_$3,new_topic_(.*)=$1"
    );

    String testTopicName = "new_topic_abc.def";
    Map<String, TableId> topicsToTables = new HashMap<>();
    // Create shallow copy of map, deep copy not needed.
    Map<String, TableId> expectedTopicsToTables = new HashMap<>(topicsToTables);
    expectedTopicsToTables.put(testTopicName, TableId.of("scratch", "abc_def"));

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.updateTopicToTable(testConfig, testTopicName, topicsToTables);

    assertEquals(expectedTopicsToTables, topicsToTables);
  }

  @Test(expected = ConfigException.class)
  public void testUpdateTopicToTableWithInvalidRegex() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        ".*=$1"
    );

    String testTopicName = "new_topic_abc.def";
    Map<String, TableId> topicsToTables = new HashMap<>();

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.updateTopicToTable(testConfig, testTopicName, topicsToTables);
  }

  @Test(expected = ConfigException.class)
  public void testUpdateTopicToTableWithMultipleMatches() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "(.*)=$1,new_topic_(.*)=$1"
    );

    String testTopicName = "new_topic_abc.def";
    Map<String, TableId> topicsToTables = new HashMap<>();

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.updateTopicToTable(testConfig, testTopicName, topicsToTables);
  }

  @Test
  public void testBaseTablesToTopics() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(BigQuerySinkConfig.DATASETS_CONFIG, ".*=scratch");
    configProperties.put(BigQuerySinkConfig.TOPICS_CONFIG, "sanitize-me,leave_me_alone");
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    Map<TableId, String> testTablesToSchemas =
        TopicToTableResolver.getBaseTablesToTopics(testConfig);
    Map<TableId, String> expectedTablesToSchemas = new HashMap<>();
    expectedTablesToSchemas.put(TableId.of("scratch", "sanitize_me"), "sanitize-me");
    expectedTablesToSchemas.put(TableId.of("scratch", "leave_me_alone"), "leave_me_alone");
    assertEquals(expectedTablesToSchemas, testTablesToSchemas);
  }
}
