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

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;

import com.wepay.kafka.connect.bigquery.utils.Version;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;

import org.apache.kafka.common.config.ConfigValue;
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

  BigQuerySinkConfig config;
  Map<String, String> configProperties;

  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkConnector.class);

  @Override
  public ConfigDef config() {
    logger.trace("connector.config()");
    return BigQuerySinkConfig.getConfig();
  }

  @Override
  public Config validate(Map<String, String> properties) {
    List<ConfigValue> singlePropertyValidations = config().validate(properties);
    // If any of our properties had malformed syntax or failed a validation to ensure, e.g., that it fell within an
    // acceptable numeric range, we only report those errors since they prevent us from being able to construct a
    // valid BigQuerySinkConfig instance
    if (singlePropertyValidations.stream().anyMatch(v -> !v.errorMessages().isEmpty())) {
      return new Config(singlePropertyValidations);
    }
    return new BigQuerySinkConfig(properties).validate();
  }

  @Override
  public void start(Map<String, String> properties) {
    logger.trace("connector.start()");
    configProperties = properties;
    config = new BigQuerySinkConfig(properties);
    // Revalidate here in case the connector has been upgraded and its old config is no longer valid
    config.ensureValid();
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
      if (i == 0 && !config.getList(BigQuerySinkConfig.ENABLE_BATCH_CONFIG).isEmpty()) {
        // if batch loading is enabled, configure first task to do the GCS -> BQ loading
        taskConfig.put(BigQuerySinkTaskConfig.GCS_BQ_TASK_CONFIG, "true");
      }
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
