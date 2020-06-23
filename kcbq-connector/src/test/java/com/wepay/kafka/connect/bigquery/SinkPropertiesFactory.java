package com.wepay.kafka.connect.bigquery;

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


import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory for generating default configuration maps, useful for testing.
 */
public class SinkPropertiesFactory {
  /**
   * A default configuration map for the tested class.
   */
  public Map<String, String> getProperties() {
    Map<String, String> properties = new HashMap<>();

    properties.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "false");
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, "kcbq-test");
    properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "test-project");
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "kcbq-test");

    properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, "key.json");

    properties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "false");
    properties.put(BigQuerySinkConfig.AVRO_DATA_CACHE_SIZE_CONFIG, "10");

    properties.put(BigQuerySinkConfig.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG, "false");
    properties.put(BigQuerySinkConfig.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG, "false");

    return properties;
  }

  /**
   * Make sure that each of the default configuration properties work nicely with the given
   * configuration object.
   *
   * @param config The config object to test
   */
  public void testProperties(BigQuerySinkConfig config) {

    config.getList(config.TOPICS_CONFIG);
    config.getString(config.DEFAULT_DATASET_CONFIG);

    config.getKeyFile();
    config.getString(config.PROJECT_CONFIG);

    config.getBoolean(config.SANITIZE_TOPICS_CONFIG);
    config.getInt(config.AVRO_DATA_CACHE_SIZE_CONFIG);

    config.getBoolean(config.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG);
    config.getBoolean(config.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG);
  }
}
