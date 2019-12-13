package com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever;

import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
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
    final String testSubjectValue = "kafka-topic-value";
    final String testSubjectKey = "kafka-topic-key";
    final String testAvroSchemaString =
        "{\"type\": \"record\", "
        + "\"name\": \"testrecord\", "
        + "\"fields\": [{\"name\": \"f1\", \"type\": \"string\"}]}";
    final SchemaMetadata testSchemaMetadata = new SchemaMetadata(1, 1, testAvroSchemaString);

    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    when(schemaRegistryClient.getLatestSchemaMetadata(testSubjectValue)).thenReturn(testSchemaMetadata);
    when(schemaRegistryClient.getLatestSchemaMetadata(testSubjectKey)).thenReturn(testSchemaMetadata);

    SchemaRegistrySchemaRetriever testSchemaRetriever = new SchemaRegistrySchemaRetriever(
        schemaRegistryClient,
        new AvroData(0)
    );

    Schema expectedKafkaConnectSchema =
        SchemaBuilder.struct().field("f1", Schema.STRING_SCHEMA).name("testrecord").build();

    assertEquals(expectedKafkaConnectSchema, testSchemaRetriever.retrieveSchema(table, testTopic, KafkaSchemaRecordType.VALUE));
    assertEquals(expectedKafkaConnectSchema, testSchemaRetriever.retrieveSchema(table, testTopic, KafkaSchemaRecordType.KEY));
  }
}
