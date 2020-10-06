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

package com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Class for managing the configuration properties of the Schema Registry Schema Retriever.
 */
public class SchemaRegistrySchemaRetrieverConfig extends AbstractConfig {
  private static final ConfigDef config;

  public static final String LOCATION_CONFIG =                     "schemaRegistryLocation";
  private static final ConfigDef.Type LOCATION_TYPE =              ConfigDef.Type.STRING;
  private static final ConfigDef.Importance LOCATION_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String LOCATION_DOC =
      "The base URL of the Schema Registry instance to use";

  public static final String AVRO_DATA_CACHE_SIZE_CONFIG =                 "avroDataCacheSize";
  private static final ConfigDef.Type AVRO_DATA_CACHE_SIZE_TYPE =          ConfigDef.Type.INT;
  public static final Integer AVRO_DATA_CACHE_SIZE_DEFAULT =               100;
  private static final ConfigDef.Validator AVRO_DATA_CACHE_SIZE_VALIDATOR =
      ConfigDef.Range.atLeast(0);
  private static final ConfigDef.Importance AVRO_DATA_CACHE_SIZE_IMPORTANCE =
      ConfigDef.Importance.LOW;
  private static final String AVRO_DATA_CACHE_SIZE_DOC =
      "The size of the cache to use when converting schemas from Avro to Kafka Connect";

  static {
    config = new ConfigDef()
        .define(
            LOCATION_CONFIG,
            LOCATION_TYPE,
            LOCATION_IMPORTANCE,
            LOCATION_DOC
        ).define(
            AVRO_DATA_CACHE_SIZE_CONFIG,
            AVRO_DATA_CACHE_SIZE_TYPE,
            AVRO_DATA_CACHE_SIZE_DEFAULT,
            AVRO_DATA_CACHE_SIZE_VALIDATOR,
            AVRO_DATA_CACHE_SIZE_IMPORTANCE,
            AVRO_DATA_CACHE_SIZE_DOC
        );
  }

  /**
   * @param properties A Map detailing configuration properties and their respective values.
   */
  public SchemaRegistrySchemaRetrieverConfig(Map<String, String> properties) {
    super(config, properties);
  }
}
