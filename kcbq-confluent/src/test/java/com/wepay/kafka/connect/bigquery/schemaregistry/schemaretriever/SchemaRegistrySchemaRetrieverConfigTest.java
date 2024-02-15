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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SchemaRegistrySchemaRetrieverConfigTest {
  
  @Test
  public void testClientPrefix() {
    final Map<String, String> clientProperties = new HashMap<>();
    final Map<String, String> allProperties = new HashMap<>();

    clientProperties.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "SASL_INHERIT");
    clientProperties.put(SchemaRegistryClientConfig.USER_INFO_CONFIG, "foo:bar");

    allProperties.put(SchemaRegistrySchemaRetrieverConfig.AVRO_DATA_CACHE_SIZE_CONFIG, "69");
    allProperties.put(SchemaRegistrySchemaRetrieverConfig.LOCATION_CONFIG, "http://localhost:8083");
    
    for (Map.Entry<String, String> clientConfig : clientProperties.entrySet()) {
      allProperties.put(
          SchemaRegistrySchemaRetrieverConfig.SCHEMA_REGISTRY_CLIENT_PREFIX + clientConfig.getKey(),
          clientConfig.getValue()
      );
    }
    
    SchemaRegistrySchemaRetrieverConfig config =
        new SchemaRegistrySchemaRetrieverConfig(allProperties);
    assertEquals(
        clientProperties,
        config.originalsWithPrefix(config.SCHEMA_REGISTRY_CLIENT_PREFIX)
    );
  }
}
