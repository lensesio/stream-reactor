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

import java.util.Map;

/**
 * Class for task-specific configuration properties.
 */
public class BigQuerySinkTaskConfig extends BigQuerySinkConfig {
  
  public static final String GCS_BQ_TASK_CONFIG = "GCSBQTask";
  private static final ConfigDef.Type GCS_BQ_TASK_TYPE = ConfigDef.Type.BOOLEAN;
  private static final boolean GCS_BQ_TASK_DEFAULT = false;
  private static final ConfigDef.Importance GCS_BQ_TASK_IMPORTANCE = ConfigDef.Importance.LOW;

  public static final String TASK_ID_CONFIG =                   "taskId";
  private static final ConfigDef.Type TASK_ID_TYPE =            ConfigDef.Type.INT;
  public static final ConfigDef.Importance TASK_ID_IMPORTANCE = ConfigDef.Importance.LOW;

  /**
   * Return a ConfigDef object used to define this config's fields.
   *
   * @return A ConfigDef object used to define this config's fields.
   */
  public static ConfigDef config() {
    return BigQuerySinkConfig.getConfig()
        .defineInternal(
            GCS_BQ_TASK_CONFIG,
            GCS_BQ_TASK_TYPE,
            GCS_BQ_TASK_DEFAULT,
            GCS_BQ_TASK_IMPORTANCE
        ).defineInternal(
            TASK_ID_CONFIG,
            TASK_ID_TYPE,
            ConfigDef.NO_DEFAULT_VALUE,
            TASK_ID_IMPORTANCE
        );
  }

  /**
   * @param properties A Map detailing configuration properties and their respective values.
   */
  public BigQuerySinkTaskConfig(Map<String, String> properties) {
    super(config(), properties);
  }
}
