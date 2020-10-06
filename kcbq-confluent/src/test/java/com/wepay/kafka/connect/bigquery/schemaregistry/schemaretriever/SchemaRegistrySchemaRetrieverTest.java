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

import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.TableId;

import io.confluent.connect.avro.AvroData;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.junit.Test;

public class SchemaRegistrySchemaRetrieverTest {
  @Test
  public void testRetrieveSchema() throws Exception {
    final TableId table = TableId.of("test", "kafka_topic");
    final String testTopic = "kafka-topic";
    final String testSubject = "kafka-topic-value";
    final String testAvroSchemaString =
        "{\"type\": \"record\", "
        + "\"name\": \"testrecord\", "
        + "\"fields\": [{\"name\": \"f1\", \"type\": \"string\"}]}";
    final SchemaMetadata testSchemaMetadata = new SchemaMetadata(1, 1, testAvroSchemaString);

    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    when(schemaRegistryClient.getLatestSchemaMetadata(testSubject)).thenReturn(testSchemaMetadata);

    SchemaRegistrySchemaRetriever testSchemaRetriever = new SchemaRegistrySchemaRetriever(
        schemaRegistryClient,
        new AvroData(0)
    );

    Schema expectedKafkaConnectSchema =
        SchemaBuilder.struct().field("f1", Schema.STRING_SCHEMA).name("testrecord").build();

    assertEquals(expectedKafkaConnectSchema, testSchemaRetriever.retrieveSchema(table, testTopic));
  }
}
