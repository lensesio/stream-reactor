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

package com.wepay.kafka.connect.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConnectorConfig;

import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.SinkConfigConnectException;

import com.wepay.kafka.connect.bigquery.utils.TopicToTableResolver;
import com.wepay.kafka.connect.bigquery.utils.Version;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link SinkConnector} used to delegate BigQuery data writes to
 * {@link org.apache.kafka.connect.sink.SinkTask SinkTasks}.
 */
public class BigQuerySinkConnector extends SinkConnector {
  private final BigQuery testBigQuery;
  private final SchemaManager testSchemaManager;

  public BigQuerySinkConnector() {
    testBigQuery = null;
    testSchemaManager = null;
  }

  // For testing purposes only; will never be called by the Kafka Connect framework
  BigQuerySinkConnector(BigQuery bigQuery) {
    this.testBigQuery = bigQuery;
    this.testSchemaManager = null;
  }

  // For testing purposes only; will never be called by the Kafka Connect framework
  BigQuerySinkConnector(BigQuery bigQuery, SchemaManager schemaManager) {
    this.testBigQuery = bigQuery;
    this.testSchemaManager = schemaManager;
  }

  private BigQuerySinkConnectorConfig config;
  private Map<String, String> configProperties;

  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkConnector.class);

  @Override
  public ConfigDef config() {
    logger.trace("connector.config()");
    return config.getConfig();
  }

  private BigQuery getBigQuery() {
    if (testBigQuery != null) {
      return testBigQuery;
    }
    String projectName = config.getString(config.PROJECT_CONFIG);
    String keyFilename = config.getString(config.KEYFILE_CONFIG);
    return new BigQueryHelper().connect(projectName, keyFilename);
  }

  private SchemaManager getSchemaManager(BigQuery bigQuery) {
    if (testSchemaManager != null) {
      return testSchemaManager;
    }
    SchemaRetriever schemaRetriever = config.getSchemaRetriever();
    SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter = config.getSchemaConverter();
    return new SchemaManager(schemaRetriever, schemaConverter, bigQuery);
  }

  private void ensureExistingTables(
      BigQuery bigQuery,
      SchemaManager schemaManager,
      Map<String, TableId> topicsToTableIds) {
    for (Map.Entry<String, TableId> topicToTableId : topicsToTableIds.entrySet()) {
      String topic = topicToTableId.getKey();
      TableId tableId = topicToTableId.getValue();
      if (bigQuery.getTable(tableId) == null) {
        logger.info("Table {} does not exist; attempting to create", tableId);
        schemaManager.createTable(tableId, topic);
      }
    }
  }

  private void ensureExistingTables(
      BigQuery bigQuery,
      Map<String, TableId> topicsToTableIds) {
    for (TableId tableId : topicsToTableIds.values()) {
      if (bigQuery.getTable(tableId) == null) {
        logger.warn(
            "You may want to enable auto table creation by setting {}=true in the properties file",
            config.TABLE_CREATE_CONFIG);
        throw new BigQueryConnectException("Table '" + tableId + "' does not exist");
      }
    }
  }

  private void ensureExistingTables() {
    BigQuery bigQuery = getBigQuery();
    Map<String, TableId> topicsToTableIds = TopicToTableResolver.getTopicsToTables(config);
    if (config.getBoolean(config.TABLE_CREATE_CONFIG)) {
      SchemaManager schemaManager = getSchemaManager(bigQuery);
      ensureExistingTables(bigQuery, schemaManager, topicsToTableIds);
    } else {
      ensureExistingTables(bigQuery, topicsToTableIds);
    }
  }

  @Override
  public void start(Map<String, String> properties) {
    logger.trace("connector.start()");
    try {
      configProperties = properties;
      config = new BigQuerySinkConnectorConfig(properties);
    } catch (ConfigException err) {
      throw new SinkConfigConnectException(
          "Couldn't start BigQuerySinkConnector due to configuration error",
          err
      );
    }

    ensureExistingTables();
  }

  @Override
  public void stop() {
    logger.trace("connector.stop()");
  }

  @Override
  public Class<? extends Task> taskClass() {
    logger.trace("connector.taskClass()");
    return BigQuerySinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    logger.trace("connector.taskConfigs()");
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      // Copy configProperties so that tasks can't interfere with each others' configurations
      HashMap<String, String> taskConfig = new HashMap<>(configProperties);
      taskConfigs.add(taskConfig);
    }
    return taskConfigs;
  }

  @Override
  public String version() {
    String version = Version.version();
    logger.trace("connector.version() = {}", version);
    return version;
  }
}
