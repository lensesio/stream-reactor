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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Class for connector-specific configuration properties.
 */
public class BigQuerySinkConnectorConfig extends BigQuerySinkConfig {
  private static final ConfigDef config;
  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkConnectorConfig.class);

  public static final String TABLE_CREATE_CONFIG =                     "autoCreateTables";
  private static final ConfigDef.Type TABLE_CREATE_TYPE =              ConfigDef.Type.BOOLEAN;
  public static final boolean TABLE_CREATE_DEFAULT =                   false;
  private static final ConfigDef.Importance TABLE_CREATE_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String TABLE_CREATE_DOC =
      "Automatically create BigQuery tables if they don't already exist";

  static {
    config = BigQuerySinkConfig.getConfig()
        .define(
            TABLE_CREATE_CONFIG,
            TABLE_CREATE_TYPE,
            TABLE_CREATE_DEFAULT,
            TABLE_CREATE_IMPORTANCE,
            TABLE_CREATE_DOC
        );
  }

  private void checkAutoCreateTables() {
    Class<?> schemaRetriever = getClass(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG);

    boolean autoCreateTables = getBoolean(TABLE_CREATE_CONFIG);
    if (autoCreateTables && schemaRetriever == null) {
      throw new ConfigException(
          "Cannot specify automatic table creation without a schema retriever"
      );
    }

    if (schemaRetriever == null) {
      logger.warn(
          "No schema retriever class provided; auto table creation is impossible"
      );
    }
  }

  public static ConfigDef getConfig() {
    return config;
  }

  /**
   * @param properties A Map detailing configuration properties and their respective values.
   */
  public BigQuerySinkConnectorConfig(Map<String, String> properties) {
    super(config, properties);
    checkAutoCreateTables();
  }
}
