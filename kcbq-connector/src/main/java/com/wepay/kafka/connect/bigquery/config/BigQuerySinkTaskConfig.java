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
 * Class for task-specific configuration properties.
 */
public class BigQuerySinkTaskConfig extends BigQuerySinkConfig {
  private static final ConfigDef config;
  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkTaskConfig.class);

  public static final String SCHEMA_UPDATE_CONFIG =                     "autoUpdateSchemas";
  private static final ConfigDef.Type SCHEMA_UPDATE_TYPE =              ConfigDef.Type.BOOLEAN;
  public static final Boolean SCHEMA_UPDATE_DEFAULT =                   false;
  private static final ConfigDef.Importance SCHEMA_UPDATE_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String SCHEMA_UPDATE_DOC =
      "Whether or not to automatically update BigQuery schemas";

  public static final String THREAD_POOL_SIZE_CONFIG =                  "threadPoolSize";
  private static final ConfigDef.Type THREAD_POOL_SIZE_TYPE =           ConfigDef.Type.INT;
  public static final Integer THREAD_POOL_SIZE_DEFAULT =                10;
  private static final ConfigDef.Validator THREAD_POOL_SIZE_VALIDATOR = ConfigDef.Range.atLeast(1);
  private static final ConfigDef.Importance THREAD_POOL_SIZE_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String THREAD_POOL_SIZE_DOC =
      "The size of the BigQuery write thread pool. This establishes the maximum number of "
      + "concurrent writes to BigQuery.";

  public static final String QUEUE_SIZE_CONFIG =                    "queueSize";
  private static final ConfigDef.Type QUEUE_SIZE_TYPE =             ConfigDef.Type.LONG;
  // should this even have a default?
  public static final Long QUEUE_SIZE_DEFAULT =                     -1L;
  private static final ConfigDef.Validator QUEUE_SIZE_VALIDATOR =   ConfigDef.Range.atLeast(-1);
  private static final ConfigDef.Importance QUEUE_SIZE_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String QUEUE_SIZE_DOC =
      "The maximum size (or -1 for no maximum size) of the worker queue for bigQuery write "
      + "requests before all topics are paused. This is a soft limit; the size of the queue can "
      + "go over this before topics are paused. All topics will be resumed once a flush is "
      + "requested or the size of the queue drops under half of the maximum size.";

  public static final String BIGQUERY_RETRY_CONFIG =                    "bigQueryRetry";
  private static final ConfigDef.Type BIGQUERY_RETRY_TYPE =             ConfigDef.Type.INT;
  public static final Integer BIGQUERY_RETRY_DEFAULT =                  0;
  private static final ConfigDef.Validator BIGQUERY_RETRY_VALIDATOR =   ConfigDef.Range.atLeast(0);
  private static final ConfigDef.Importance BIGQUERY_RETRY_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String BIGQUERY_RETRY_DOC =
      "The number of retry attempts that will be made per BigQuery request that fails with a "
      + "backend error or a quota exceeded error";

  public static final String BIGQUERY_RETRY_WAIT_CONFIG =               "bigQueryRetryWait";
  private static final ConfigDef.Type BIGQUERY_RETRY_WAIT_CONFIG_TYPE = ConfigDef.Type.LONG;
  public static final Long BIGQUERY_RETRY_WAIT_DEFAULT =                1000L;
  private static final ConfigDef.Validator BIGQUERY_RETRY_WAIT_VALIDATOR =
      ConfigDef.Range.atLeast(0);
  private static final ConfigDef.Importance BIGQUERY_RETRY_WAIT_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String BIGQUERY_RETRY_WAIT_DOC =
      "The minimum amount of time, in milliseconds, to wait between BigQuery backend or quota "
      +  "exceeded error retry attempts.";

  public static final String BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG = "bigQueryMessageTimePartitioning";
  private static final ConfigDef.Type BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG_TYPE = ConfigDef.Type.BOOLEAN;
  public static final Boolean BIGQUERY_MESSAGE_TIME_PARTITIONING_DEFAULT =                   false;
  private static final ConfigDef.Importance BIGQUERY_MESSAGE_TIME_PARTITIONING_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String BIGQUERY_MESSAGE_TIME_PARTITIONING_DOC =
          "Whether or not to use the message time when inserting records. Default uses the connector processing time.";

  static {
    config = BigQuerySinkConfig.getConfig()
        .define(
            SCHEMA_UPDATE_CONFIG,
            SCHEMA_UPDATE_TYPE,
            SCHEMA_UPDATE_DEFAULT,
            SCHEMA_UPDATE_IMPORTANCE,
            SCHEMA_UPDATE_DOC
        ).define(
            THREAD_POOL_SIZE_CONFIG,
            THREAD_POOL_SIZE_TYPE,
            THREAD_POOL_SIZE_DEFAULT,
            THREAD_POOL_SIZE_VALIDATOR,
            THREAD_POOL_SIZE_IMPORTANCE,
            THREAD_POOL_SIZE_DOC
        ).define(
            QUEUE_SIZE_CONFIG,
            QUEUE_SIZE_TYPE,
            QUEUE_SIZE_DEFAULT,
            QUEUE_SIZE_VALIDATOR,
            QUEUE_SIZE_IMPORTANCE,
            QUEUE_SIZE_DOC
        ).define(
            BIGQUERY_RETRY_CONFIG,
            BIGQUERY_RETRY_TYPE,
            BIGQUERY_RETRY_DEFAULT,
            BIGQUERY_RETRY_VALIDATOR,
            BIGQUERY_RETRY_IMPORTANCE,
            BIGQUERY_RETRY_DOC
        ).define(
            BIGQUERY_RETRY_WAIT_CONFIG,
            BIGQUERY_RETRY_WAIT_CONFIG_TYPE,
            BIGQUERY_RETRY_WAIT_DEFAULT,
            BIGQUERY_RETRY_WAIT_VALIDATOR,
            BIGQUERY_RETRY_WAIT_IMPORTANCE,
            BIGQUERY_RETRY_WAIT_DOC
        ).define(
            BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG,
            BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG_TYPE,
            BIGQUERY_MESSAGE_TIME_PARTITIONING_DEFAULT,
            BIGQUERY_MESSAGE_TIME_PARTITIONING_IMPORTANCE,
            BIGQUERY_MESSAGE_TIME_PARTITIONING_DOC
        );
  }

  private void checkAutoUpdateSchemas() {
    Class<?> schemaRetriever = getClass(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG);

    boolean autoUpdateSchemas = getBoolean(SCHEMA_UPDATE_CONFIG);
    if (autoUpdateSchemas && schemaRetriever == null) {
      throw new ConfigException(
          "Cannot specify automatic table creation without a schema retriever"
      );
    }

    if (schemaRetriever == null) {
      logger.warn(
          "No schema retriever class provided; auto schema updates are impossible"
      );
    }
  }

  public static ConfigDef getConfig() {
    return config;
  }

  /**
   * @param properties A Map detailing configuration properties and their respective values.
   */
  public BigQuerySinkTaskConfig(Map<String, String> properties) {
    super(config, properties);
    checkAutoUpdateSchemas();
  }
}
