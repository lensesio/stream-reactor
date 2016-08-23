package com.wepay.kafka.connect.bigquery;

/*
 * Copyright 2016 WePay, Inc.
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


import static org.junit.Assert.assertSame;

import static org.mockito.Matchers.any;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

import com.wepay.kafka.connect.bigquery.convert.BigQuerySchemaConverter;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import com.wepay.kafka.connect.bigquery.exception.SchemaRegistryConnectException;

import io.confluent.connect.avro.AvroData;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class SchemaManagerTest {
  @Test
  public void testCreateTable() throws Exception {
    final String topic = "create-table-test";
    final String dataset = "data";
    final TableId table = TableId.of(dataset, "create_table_test");
    final String schemaName = topic + "-value";
    final String fakeSchemaString =
        "{\"type\":\"record\","
            + "\"name\":\"testrecord\","
            + "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
    final org.apache.kafka.connect.data.Schema fakeKafkaConnectSchema =
        mock(org.apache.kafka.connect.data.Schema.class);
    final Schema fakeBigQuerySchema = Schema.of(Field.of("mock field", Field.Type.string()));
    final Table fakeTable = mock(Table.class);

    Map<TableId, String> tablesToTopics = Collections.singletonMap(table, topic);

    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    SchemaMetadata fakeSchemaMetadata = new SchemaMetadata(1, 1, fakeSchemaString);
    when(schemaRegistryClient.getLatestSchemaMetadata(schemaName)).thenReturn(fakeSchemaMetadata);

    AvroData avroData = mock(AvroData.class);
    when(avroData.toConnectSchema(any(org.apache.avro.Schema.class)))
        .thenReturn(fakeKafkaConnectSchema);

    SchemaConverter<Schema> schemaConverter = mock(BigQuerySchemaConverter.class);
    when(schemaConverter.convertSchema(fakeKafkaConnectSchema)).thenReturn(fakeBigQuerySchema);

    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.create(any(TableInfo.class))).thenReturn(fakeTable);

    SchemaManager testSchemaManager = new SchemaManager(
        tablesToTopics,
        schemaRegistryClient,
        avroData,
        schemaConverter,
        bigQuery
    );

    assertSame(fakeTable, testSchemaManager.createTable(table));

    verify(schemaRegistryClient).getLatestSchemaMetadata(schemaName);
    verify(avroData).toConnectSchema(any(org.apache.avro.Schema.class));
    verify(schemaConverter).convertSchema(fakeKafkaConnectSchema);
    verify(bigQuery).create(any(TableInfo.class));
  }

  @Test
  public void testUpdateTable() throws Exception {
    final String topic = "update-table-test";
    final String dataset = "data";
    final TableId table = TableId.of(dataset, "update_table_test");
    final String schemaName = topic + "-value";
    final String fakeSchemaString =
        "{\"type\":\"record\","
        + "\"name\":\"testrecord\","
        + "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
    final org.apache.kafka.connect.data.Schema fakeKafkaConnectSchema =
        mock(org.apache.kafka.connect.data.Schema.class);
    final Schema fakeBigQuerySchema = Schema.of(Field.of("mock field", Field.Type.string()));
    final Table fakeTable = mock(Table.class);

    Map<TableId, String> tablesToTopics = Collections.singletonMap(table, topic);

    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    SchemaMetadata fakeSchemaMetadata = new SchemaMetadata(1, 1, fakeSchemaString);
    when(schemaRegistryClient.getLatestSchemaMetadata(schemaName)).thenReturn(fakeSchemaMetadata);

    AvroData avroData = mock(AvroData.class);
    when(avroData.toConnectSchema(any(org.apache.avro.Schema.class)))
        .thenReturn(fakeKafkaConnectSchema);

    SchemaConverter<Schema> schemaConverter = mock(BigQuerySchemaConverter.class);
    when(schemaConverter.convertSchema(fakeKafkaConnectSchema)).thenReturn(fakeBigQuerySchema);

    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.update(any(TableInfo.class))).thenReturn(fakeTable);

    SchemaManager testSchemaManager = new SchemaManager(
        tablesToTopics,
        schemaRegistryClient,
        avroData,
        schemaConverter,
        bigQuery
    );

    assertSame(fakeTable, testSchemaManager.updateTable(table));

    verify(schemaRegistryClient).getLatestSchemaMetadata(schemaName);
    verify(avroData).toConnectSchema(any(org.apache.avro.Schema.class));
    verify(schemaConverter).convertSchema(fakeKafkaConnectSchema);
    verify(bigQuery).update(any(TableInfo.class));
  }

  @Test(expected = SchemaRegistryConnectException.class)
  public void testCreateTableFailure() throws Exception {
    final String topic = "create-table-failure-test";
    final String dataset = "data";
    final TableId table = TableId.of(dataset, "create_table_failure_test");
    final String schemaName = topic + "-value";

    Map<TableId, String> tablesToTopics = Collections.singletonMap(table, topic);

    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    RestClientException restClientException =
        new RestClientException("Schema not found", 404, 404);
    when(schemaRegistryClient.getLatestSchemaMetadata(schemaName))
        .thenThrow(restClientException);

    new SchemaManager(
        tablesToTopics,
        schemaRegistryClient,
        mock(AvroData.class),
        mock(BigQuerySchemaConverter.class),
        mock(BigQuery.class)
    ).createTable(table);
  }

  @Test(expected = SchemaRegistryConnectException.class)
  public void testUpdateTableFailure() throws Exception {
    final String topic = "update-table-failure-test";
    final String dataset = "data";
    final TableId table = TableId.of(dataset, "update_table_failure_test");
    final String schemaName = topic + "-value";

    Map<TableId, String> tablesToTopics = Collections.singletonMap(table, topic);

    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    RestClientException restClientException =
        new RestClientException("Schema not found", 404, 404);
    when(schemaRegistryClient.getLatestSchemaMetadata(schemaName))
        .thenThrow(restClientException);

    new SchemaManager(
        tablesToTopics,
        schemaRegistryClient,
        mock(AvroData.class),
        mock(BigQuerySchemaConverter.class),
        mock(BigQuery.class)
    ).updateTable(table);
  }
}
