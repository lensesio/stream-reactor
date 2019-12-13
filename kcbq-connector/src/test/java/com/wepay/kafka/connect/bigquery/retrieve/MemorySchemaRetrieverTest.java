package com.wepay.kafka.connect.bigquery.retrieve;


import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;


public class MemorySchemaRetrieverTest {
  public TableId getTableId(String datasetName, String tableName) {
    return TableId.of(datasetName, tableName);
  }

  @Test
  public void testRetrieveSchemaWhenNoLastSeenSchemaReturnsEmptyStructSchema() {
    final String topic = "test-retrieve";
    final TableId tableId = getTableId("testTable", "testDataset");
    SchemaRetriever retriever = new MemorySchemaRetriever();
    retriever.configure(new HashMap<>());
    Assert.assertEquals(retriever.retrieveSchema(tableId, topic, KafkaSchemaRecordType.VALUE), SchemaBuilder.struct().build());
  }

  @Test
  public void testRetrieveSchemaWhenLastSeenExistsSucceeds() {
    final String topic = "test-retrieve";
    final TableId tableId = getTableId("testTable", "testDataset");
    SchemaRetriever retriever = new MemorySchemaRetriever();
    retriever.configure(new HashMap<>());

    Schema expectedSchema = Schema.OPTIONAL_FLOAT32_SCHEMA;
    retriever.setLastSeenSchema(tableId, topic, expectedSchema);

    Assert.assertEquals(retriever.retrieveSchema(tableId, topic, KafkaSchemaRecordType.KEY), expectedSchema);
  }

  @Test
  public void testRetrieveSchemaWithMultipleSchemasSucceeds() {
    final String floatSchemaTopic = "test-float32";
    final String intSchemaTopic = "test-int32";
    final TableId floatTableId = getTableId("testFloatTable", "testFloatDataset");
    final TableId intTableId = getTableId("testIntTable", "testIntDataset");
    SchemaRetriever retriever = new MemorySchemaRetriever();
    retriever.configure(new HashMap<>());

    Schema expectedIntSchema = Schema.INT32_SCHEMA;
    Schema expectedFloatSchema = Schema.OPTIONAL_FLOAT32_SCHEMA;
    retriever.setLastSeenSchema(floatTableId, floatSchemaTopic, expectedFloatSchema);
    retriever.setLastSeenSchema(intTableId, intSchemaTopic, expectedIntSchema);

    Assert.assertEquals(
        retriever.retrieveSchema(floatTableId, floatSchemaTopic, KafkaSchemaRecordType.KEY), expectedFloatSchema);
    Assert.assertEquals(retriever.retrieveSchema(intTableId, intSchemaTopic, KafkaSchemaRecordType.KEY), expectedIntSchema);
  }

  @Test
  public void testRetrieveSchemaRetrievesLastSeenSchema() {
    final String intSchemaTopic = "test-int";
    final TableId tableId = getTableId("testTable", "testDataset");
    SchemaRetriever retriever = new MemorySchemaRetriever();
    retriever.configure(new HashMap<>());

    Schema firstSchema = Schema.INT32_SCHEMA;
    Schema secondSchema = Schema.INT64_SCHEMA;
    retriever.setLastSeenSchema(tableId, intSchemaTopic, firstSchema);
    retriever.setLastSeenSchema(tableId, intSchemaTopic, secondSchema);

    Assert.assertEquals(retriever.retrieveSchema(tableId, intSchemaTopic, KafkaSchemaRecordType.VALUE), secondSchema);
  }
}
