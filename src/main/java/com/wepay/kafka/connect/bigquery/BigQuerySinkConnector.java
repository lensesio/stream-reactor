package com.wepay.kafka.connect.bigquery;

/*
 * Copyright 2016 Wepay, Inc.
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


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConnectorConfig;


import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.SinkConfigConnectException;

import com.wepay.kafka.connect.bigquery.utils.Version;

import io.confluent.connect.avro.AvroData;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;

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

  private SchemaManager getSchemaManager(Map<TableId, String> tablesToTopics, BigQuery bigQuery) {
    if (testSchemaManager != null) {
      return testSchemaManager;
    }

    // Don't want to do ANY caching in the schema registry client, since we want to handle the
    // possibility of schemas changing during the lifetime of the connector
    return new SchemaManager(
        tablesToTopics,
        new CachedSchemaRegistryClient(config.getString(config.REGISTRY_CONFIG), 0),
        new AvroData(config.getInt(config.AVRO_DATA_CACHE_SIZE_CONFIG)),
        config.getSchemaConverter(),
        bigQuery
    );
  }

  private void ensureExistingTables(BigQuery bigQuery,
                                    SchemaManager schemaManager,
                                    Map<String, String> topicsToDatasets) {
    for (Map.Entry<String, String> topicDataset : topicsToDatasets.entrySet()) {
      String topic = topicDataset.getKey();
      String table = config.getTableFromTopic(topic);
      String dataset = topicDataset.getValue();
      TableId tableId = TableId.of(dataset, table);
      if (bigQuery.getTable(tableId) == null) {
        logger.info("Table {} does not exist; attempting to create", tableId);
        schemaManager.createTable(tableId);
      }
    }
  }

  private void ensureExistingTables(BigQuery bigQuery, Map<String, String> topicsToDatasets) {
    for (Map.Entry<String, String> topicDataset : topicsToDatasets.entrySet()) {
      String topic = topicDataset.getKey();
      String table = config.getTableFromTopic(topic);
      String dataset = topicDataset.getValue();
      TableId tableId = TableId.of(dataset, table);
      if (bigQuery.getTable(tableId) == null) {
        logger.warn(
              "You may want to enable auto table creation by specifying"
              + "{}=true in the properties file",
              config.TABLE_CREATE_CONFIG
        );
        throw new BigQueryConnectException("Table '" + tableId + "' does not exist");
      }
    }
  }

  private void ensureExistingTables() {
    BigQuery bigQuery = getBigQuery();
    Map<String, String> topicsToDatasets = config.getTopicsToDatasets();
    if (config.getBoolean(config.TABLE_CREATE_CONFIG)) {
      Map<TableId, String> tablesToTopics = config.getTablesToTopics(topicsToDatasets);
      SchemaManager schemaManager = getSchemaManager(tablesToTopics, bigQuery);
      ensureExistingTables(bigQuery, schemaManager, topicsToDatasets);
    } else {
      ensureExistingTables(bigQuery, topicsToDatasets);
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
    int maxWritePerTask = config.getInt(config.MAX_WRITE_CONFIG) / maxTasks;
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      // Copy configProperties so that tasks can't interfere with each others' configurations
      HashMap<String, String> taskConfig = new HashMap<>(configProperties);
      taskConfig.put(config.MAX_WRITE_CONFIG, Integer.toString(maxWritePerTask));
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
