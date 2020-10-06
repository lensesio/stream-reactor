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

package com.wepay.kafka.connect.bigquery;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import org.apache.kafka.connect.data.Schema;

import org.junit.Assert;
import org.junit.Test;

public class SchemaManagerTest {

  @Test
  public void testBQTableDescription() {
    final String testTableName = "testTable";
    final String testDatasetName = "testDataset";
    final String testDoc = "test doc";
    final TableId tableId = TableId.of(testDatasetName, testTableName);

    SchemaRetriever mockSchemaRetriever = mock(SchemaRetriever.class);
    @SuppressWarnings("unchecked")
    SchemaConverter<com.google.cloud.bigquery.Schema> mockSchemaConverter =
        (SchemaConverter<com.google.cloud.bigquery.Schema>) mock(SchemaConverter.class);
    BigQuery mockBigQuery = mock(BigQuery.class);

    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever,
                                                    mockSchemaConverter,
                                                    mockBigQuery);

    Schema mockKafkaSchema = mock(Schema.class);
    // we would prefer to mock this class, but it is final.
    com.google.cloud.bigquery.Schema fakeBigQuerySchema =
        com.google.cloud.bigquery.Schema.of(Field.of("mock field", Field.Type.string()));

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager.constructTableInfo(tableId, mockKafkaSchema);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
                        testDoc, tableInfo.getDescription());
  }
}
