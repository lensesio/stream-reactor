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

package com.wepay.kafka.connect.bigquery.retrieve;

import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
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
        Assert.assertEquals(retriever.retrieveSchema(tableId, topic), SchemaBuilder.struct().build());
    }

    @Test
    public void testRetrieveSchemaWhenLastSeenExistsSucceeds() {
        final String topic = "test-retrieve";
        final TableId tableId = getTableId("testTable", "testDataset");
        SchemaRetriever retriever = new MemorySchemaRetriever();
        retriever.configure(new HashMap<>());

        Schema expectedSchema = Schema.OPTIONAL_FLOAT32_SCHEMA;
        retriever.setLastSeenSchema(tableId, topic, expectedSchema);

        Assert.assertEquals(retriever.retrieveSchema(tableId, topic), expectedSchema);
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

        Assert.assertEquals(retriever.retrieveSchema(floatTableId, floatSchemaTopic), expectedFloatSchema);
        Assert.assertEquals(retriever.retrieveSchema(intTableId, intSchemaTopic), expectedIntSchema);
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

        Assert.assertEquals(retriever.retrieveSchema(tableId, intSchemaTopic), secondSchema);
    }
}
