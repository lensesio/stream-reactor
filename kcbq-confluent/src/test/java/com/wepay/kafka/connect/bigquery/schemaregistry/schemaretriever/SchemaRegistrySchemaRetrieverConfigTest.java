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
