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
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

import com.google.cloud.bigquery.TimePartitioning;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import org.apache.kafka.connect.data.Schema;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import java.util.Arrays;
import java.util.Collections;
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
        mockBigQuery, false, false, false, kafkaKeyFieldName, kafkaDataFieldName,
        Optional.empty(), Optional.empty(), Optional.empty(), TimePartitioning.Type.DAY);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc, true);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    Assert.assertNull("Timestamp partition field name is not null",
        ((StandardTableDefinition) tableInfo.getDefinition()).getTimePartitioning().getField());
    Assert.assertNull("Partition expiration is not null",
        ((StandardTableDefinition) tableInfo.getDefinition()).getTimePartitioning().getExpirationMs());
  }

  @Test
  public void testTimestampPartitionSet() {
    Optional<String> testField = Optional.of("testField");
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, Optional.empty(), Optional.empty(), testField,
        Optional.empty(), Optional.empty(), TimePartitioning.Type.DAY);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc, true);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    StandardTableDefinition definition = tableInfo.getDefinition();
    Assert.assertNotNull(definition.getTimePartitioning());
    Assert.assertEquals(TimePartitioning.Type.DAY, definition.getTimePartitioning().getType());
    Assert.assertEquals("The field name does not match the field name of time partition",
        testField.get(),
        definition.getTimePartitioning().getField());
    Assert.assertNull("Partition expiration is not null",
            ((StandardTableDefinition) tableInfo.getDefinition()).getTimePartitioning().getExpirationMs());
  }

  @Test
  public void testAlternativeTimestampPartitionType() {
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty(), TimePartitioning.Type.HOUR);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc, true);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    StandardTableDefinition definition = tableInfo.getDefinition();
    Assert.assertNotNull(definition.getTimePartitioning());
    Assert.assertEquals(TimePartitioning.Type.HOUR, definition.getTimePartitioning().getType());
  }

  @Test
  public void testUpdateTimestampPartitionNull() {
    Optional<String> testField = Optional.of("testField");
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, Optional.empty(), Optional.empty(), testField,
        Optional.empty(), Optional.empty(), TimePartitioning.Type.DAY);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc, false);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    Assert.assertNull("The time partitioning object should be null",
        ((StandardTableDefinition) tableInfo.getDefinition()).getTimePartitioning());
  }

  @Test
  public void testUpdateTimestampPartitionNotSet() {
    Optional<String> testField = Optional.of("testField");
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, Optional.empty(), Optional.empty(), testField,
        Optional.empty(), Optional.empty(),TimePartitioning.Type.DAY);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc, true);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    StandardTableDefinition definition = tableInfo.getDefinition();
    Assert.assertNotNull(definition.getTimePartitioning());
    Assert.assertEquals("The field name does not match the field name of time partition",
        testField.get(),
        definition.getTimePartitioning().getField());

    Optional<String> updateField = Optional.of("testUpdateField");
    schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, Optional.empty(), Optional.empty(), updateField, Optional.empty(), Optional.empty(),
        TimePartitioning.Type.DAY);

    tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc, false);
    definition = tableInfo.getDefinition();
    Assert.assertNull("The time partitioning object should be null",
        ((StandardTableDefinition) tableInfo.getDefinition()).getTimePartitioning());
  }

  @Test
  public void testPartitionExpirationSetWithoutFieldName() {
    Optional<Long> testExpirationMs = Optional.of(86400000L);
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, Optional.empty(), Optional.empty(), Optional.empty(),
        testExpirationMs, Optional.empty(), TimePartitioning.Type.DAY);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc, true);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    StandardTableDefinition tableDefinition = (StandardTableDefinition) tableInfo.getDefinition();
    Assert.assertEquals("The partition expiration does not match the expiration in ms",
        testExpirationMs.get(),
        tableDefinition.getTimePartitioning().getExpirationMs());
    Assert.assertNull("Timestamp partition field name is not null",
        tableDefinition.getTimePartitioning().getField());
  }

  @Test
  public void testPartitionExpirationSetWithFieldName() {
    Optional<Long> testExpirationMs = Optional.of(86400000L);
    Optional<String> testField = Optional.of("testField");
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, Optional.empty(), Optional.empty(), testField,
        testExpirationMs, Optional.empty(), TimePartitioning.Type.DAY);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc, true);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    StandardTableDefinition tableDefinition = (StandardTableDefinition) tableInfo.getDefinition();
    Assert.assertEquals("The partition expiration does not match the expiration in ms",
        testExpirationMs.get(),
        tableDefinition.getTimePartitioning().getExpirationMs());
    Assert.assertEquals("The field name does not match the field name of time partition",
        testField.get(),
        tableDefinition.getTimePartitioning().getField());
  }

  @Test
  public void testClusteringPartitionSet() {
    Optional<String> timestampPartitionFieldName = Optional.of("testField");
    Optional<List<String>> testField = Optional.of(Arrays.asList("column1", "column2"));
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, Optional.empty(), Optional.empty(), timestampPartitionFieldName,
        Optional.empty(), testField, TimePartitioning.Type.DAY);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc, true);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    StandardTableDefinition definition = tableInfo.getDefinition();
    Assert.assertNotNull(definition.getClustering());
    Assert.assertEquals("The field name does not match the field name of time partition",
        testField.get(),
        definition.getClustering().getFields());
  }

  @Test
  public void testUpdateClusteringPartitionNull() {
    Optional<String> timestampPartitionFieldName = Optional.of("testField");
    Optional<List<String>> testField = Optional.of(Arrays.asList("column1", "column2"));
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, Optional.empty(), Optional.empty(), timestampPartitionFieldName,
        Optional.empty(), testField, TimePartitioning.Type.DAY);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc, false);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    StandardTableDefinition definition = tableInfo.getDefinition();
    Assert.assertNull("The clustering object should be null", definition.getClustering());
  }

  @Test
  public void testUpdateClusteringPartitionNotSet() {
    Optional<String> timestampPartitionFieldName = Optional.of("testField");
    Optional<List<String>> testField = Optional.of(Arrays.asList("column1", "column2"));
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, Optional.empty(), Optional.empty(), timestampPartitionFieldName,
        Optional.empty(), testField, TimePartitioning.Type.DAY);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc, true);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    StandardTableDefinition definition = tableInfo.getDefinition();
    Assert.assertNotNull(definition.getClustering());
    Assert.assertEquals("The field name should not match the field name of time partition",
        testField.get(),
        definition.getClustering().getFields());

    Optional<List<String>> updateTestField = Optional.of(Arrays.asList("column3", "column4"));
    schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, Optional.empty(), Optional.empty(), timestampPartitionFieldName,
        Optional.empty(), updateTestField, TimePartitioning.Type.DAY);

    tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc, false);
    definition = tableInfo.getDefinition();
    Assert.assertNull("The clustering object should be null", definition.getClustering());
  }

  @Test
  public void testSuccessfulUpdateWithOnlyRelaxedFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema relaxedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(false, true, false);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, relaxedSchema, relaxedSchema);
  }

  @Test(expected = BigQueryConnectException.class)
  public void testDisallowedUpdateWithOnlyRelaxedFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema relaxedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, false, false);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, relaxedSchema, null);
  }

  @Test
  public void testSuccessfulUpdateWithOnlyNewFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, false, false);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, expandedSchema);
  }

  @Test(expected = BigQueryConnectException.class)
  public void testDisallowedUpdateWithOnlyNewFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(false, true, false);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, null);
  }

  @Test(expected = BigQueryConnectException.class)
  public void testDisallowedUpdateWithOnlyNewRequiredFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, false, false);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, null);
  }

  @Test
  public void testSuccessfulUpdateWithNewAndRelaxedFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedAndRelaxedSchema = com.google.cloud.bigquery.Schema.of(
        // Relax an existing field from required to nullable
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build(),
        // Add a new nullable field
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
        // Add a new required field (that should be relaxed to nullable automatically)
        Field.newBuilder("f3", LegacySQLTypeName.NUMERIC).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("f3", LegacySQLTypeName.NUMERIC).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, false);

    testGetAndValidateProposedSchema
        (schemaManager, existingSchema, expandedAndRelaxedSchema, expectedSchema);
  }

  @Test
  public void testSuccessfulUnionizedUpdateWithNewAndRelaxedFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema disjointSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, true);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, disjointSchema, expectedSchema);
  }

  @Test
  public void testSuccessfulUnionizedUpdateWithNewRepeatedField() {
    com.google.cloud.bigquery.Schema reducedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REPEATED).build()
    );

    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REPEATED).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, true);

    // Unionization should work symmetrically, so test both cases of reduced/expanded as the current/new schemas
    testGetAndValidateProposedSchema(schemaManager, reducedSchema, expandedSchema, expectedSchema);
    testGetAndValidateProposedSchema(schemaManager, expandedSchema, reducedSchema, expectedSchema);
  }

  @Test
  public void testSuccessfulUpdateWithNewRepeatedField() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REPEATED).build()
    );

    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REPEATED).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, false);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, expectedSchema);
  }

  @Test(expected = BigQueryConnectException.class)
  public void testDisallowedUnionizedUpdateWithNewField() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(false, true, true);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, null);
  }

  @Test(expected = BigQueryConnectException.class)
  public void testDisallowedUnionizedUpdateWithRelaxedField() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, false, true);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, null);
  }

  @Test
  public void testUnionizedUpdateWithMultipleSchemas() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema firstNewSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
    );
    com.google.cloud.bigquery.Schema secondNewSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
    );
    com.google.cloud.bigquery.Schema thirdNewSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );
    List<com.google.cloud.bigquery.Schema> newSchemas =
        Arrays.asList(firstNewSchema, secondNewSchema, thirdNewSchema);

    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, true);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, newSchemas, expectedSchema);
  }

  @Test
  public void FieldsWithUnspecifiedModeShouldNotCauseNpe() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).build()
    );

    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Mode.NULLABLE).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, true);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, expectedSchema);
  }

  private SchemaManager createSchemaManager(
      boolean allowNewFields, boolean allowFieldRelaxation, boolean allowUnionization) {
    return new SchemaManager(new IdentitySchemaRetriever(), mockSchemaConverter, mockBigQuery,
        allowNewFields, allowFieldRelaxation, allowUnionization,
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        TimePartitioning.Type.DAY);
  }

  private void testGetAndValidateProposedSchema(
      SchemaManager schemaManager,
      com.google.cloud.bigquery.Schema existingSchema,
      com.google.cloud.bigquery.Schema newSchema,
      com.google.cloud.bigquery.Schema expectedSchema) {
    testGetAndValidateProposedSchema(
        schemaManager, existingSchema, Collections.singletonList(newSchema), expectedSchema);
  }

  private void testGetAndValidateProposedSchema(
      SchemaManager schemaManager,
      com.google.cloud.bigquery.Schema existingSchema,
      List<com.google.cloud.bigquery.Schema> newSchemas,
      com.google.cloud.bigquery.Schema expectedSchema) {
    Table existingTable = existingSchema != null ? tableWithSchema(existingSchema) : null;

    SinkRecord mockSinkRecord = recordWithValueSchema(mockKafkaSchema);
    List<SinkRecord> incomingSinkRecords = Collections.nCopies(newSchemas.size(), mockSinkRecord);

    when(mockBigQuery.getTable(tableId)).thenReturn(existingTable);
    OngoingStubbing<com.google.cloud.bigquery.Schema> converterStub =
        when(mockSchemaConverter.convertSchema(mockKafkaSchema));
    for (com.google.cloud.bigquery.Schema newSchema : newSchemas) {
      // The converter will return the schemas in the order that they are provided to it with the
      // call to "thenReturn"
      converterStub = converterStub.thenReturn(newSchema);
    }

    com.google.cloud.bigquery.Schema proposedSchema =
        schemaManager.getAndValidateProposedSchema(tableId, incomingSinkRecords);

    if (expectedSchema != null) {
      Assert.assertEquals(expectedSchema, proposedSchema);
    }
  }

  private Table tableWithSchema(com.google.cloud.bigquery.Schema schema) {
    TableDefinition definition = mock(TableDefinition.class);
    when(definition.getSchema()).thenReturn(schema);

    Table result = mock(Table.class);
    when(result.getDefinition()).thenReturn(definition);

    return result;
  }

  private SinkRecord recordWithValueSchema(Schema valueSchema) {
    SinkRecord result = mock(SinkRecord.class);
    when(result.valueSchema()).thenReturn(valueSchema);
    return result;
  }
}
