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
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import org.apache.kafka.connect.data.Schema;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class SchemaManagerTest {

  private String testTableName = "testTable";
  private String testDatasetName = "testDataset";
  private String testDoc = "test doc";
  private TableId tableId = TableId.of(testDatasetName, testTableName);

  private SchemaRetriever mockSchemaRetriever;
  private SchemaConverter<com.google.cloud.bigquery.Schema> mockSchemaConverter;
  private BigQuery mockBigQuery;
  private Schema mockKafkaSchema;
  private com.google.cloud.bigquery.Schema fakeBigQuerySchema;

  @Before
  public void before() {
    mockSchemaRetriever = mock(SchemaRetriever.class);
    mockSchemaConverter =
        (SchemaConverter<com.google.cloud.bigquery.Schema>) mock(SchemaConverter.class);
    mockBigQuery = mock(BigQuery.class);
    mockKafkaSchema = mock(Schema.class);
    fakeBigQuerySchema = com.google.cloud.bigquery.Schema.of(
        Field.of("mock field", LegacySQLTypeName.STRING));
  }

  @Test
  public void testBQTableDescription() {
    Optional<String> kafkaKeyFieldName = Optional.of("kafkaKey");
    Optional<String> kafkaDataFieldName = Optional.of("kafkaData");
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, kafkaKeyFieldName, kafkaDataFieldName, Optional.empty(), Optional.empty());

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, mockKafkaSchema, mockKafkaSchema);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    Assert.assertNull("Timestamp partition field name is not null",
        ((StandardTableDefinition) tableInfo.getDefinition()).getTimePartitioning().getField());
  }

  @Test
  public void testTimestampPartitionSet() {
    Optional<String> testField = Optional.of("testField");
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, Optional.empty(), Optional.empty(), testField, Optional.empty());

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, mockKafkaSchema, mockKafkaSchema);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    Assert.assertEquals("The field name does not match the field name of time partition",
        testField.get(),
        ((StandardTableDefinition) tableInfo.getDefinition()).getTimePartitioning().getField());
  }

  @Test
  public void testClusteringPartitionSet() {
    Optional<String> timestampPartitionFieldName = Optional.of("testField");
    Optional<List<String>> testField = Optional.of(Arrays.asList("column1", "column2"));
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, Optional.empty(), Optional.empty(), timestampPartitionFieldName, testField);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, mockKafkaSchema, mockKafkaSchema);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    StandardTableDefinition definition = tableInfo.getDefinition();
    Assert.assertNotNull(definition.getClustering());
    Assert.assertEquals("The field name does not match the field name of time partition",
        testField.get(),
        definition.getClustering().getFields());
  }
}
