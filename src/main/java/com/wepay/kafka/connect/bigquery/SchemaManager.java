package com.wepay.kafka.connect.bigquery;

/*
 * Copyright 2016 Wepay, Inc.
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


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;
import com.wepay.kafka.connect.bigquery.exception.SchemaRegistryConnectException;

import io.confluent.connect.avro.AvroData;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.apache.avro.Schema.Parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.Map;

/**
 * Class for managing BigQuery tables by either creating them from scratch or updating their schemas
 * via integration with Schema Registry.
 */
public class SchemaManager {
  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkConnector.class);

  private final Map<TableId, String> tablesToTopics;
  private final SchemaRegistryClient schemaRegistryClient;
  private final AvroData avroData;
  private final SchemaConverter<Schema> schemaConverter;
  private final BigQuery bigQuery;

  /**
   * @param tablesToTopics A map detailing the name of the topic that supplies data for each table.
   * @param schemaRegistryClient A client for accessing the Schema Registry. Used to retrieve schema
   *                             information from the registry when creating new tables or when
   *                             attempting to update existing tables.
   * @param avroData An AvroData object for converting between Avro schemas and Kafka Connect
   *                 schemas.
   * @param schemaConverter A converter used to convert Kafka Connect schemas to BigQuery schemas.
   * @param bigQuery A BigQuery object for interacting with BigQuery. Will be used to check for the
   *                 existence of tables, and to send create/update requests for tables.
   */
  public SchemaManager(
      Map<TableId, String> tablesToTopics,
      SchemaRegistryClient schemaRegistryClient,
      AvroData avroData,
      SchemaConverter<Schema> schemaConverter,
      BigQuery bigQuery) {
    this.tablesToTopics = tablesToTopics;
    this.schemaRegistryClient = schemaRegistryClient;
    this.avroData = avroData;
    this.schemaConverter = schemaConverter;
    this.bigQuery = bigQuery;
  }

  /**
   * Creates a new BigQuery table with the specified name in the specified dataset, fetching schema
   * information from the Schema Registry for the specified schema. If the SchemaRegistryClient
   * passed in to the constructor is null, causes an exception to be thrown.
   * This method is thread-safe and can be called concurrently.
   *
   * @param table The TableId object used to identify the table,
   * @return The resulting BigQuery Table object.
   */
  public Table createTable(TableId table) {
    logger.info("Attempting to create table {}", table);
    try {
      TableInfo tableInfo = getTableInfo(table);
      logger.trace("Sending table schema to BigQuery");
      return bigQuery.create(tableInfo);
    } catch (Exception err) {
      throw new SchemaRegistryConnectException("Failed to create new table", err);
    }
  }

  /**
   * Updates an existing BigQuery table with the specified name in the specified dataset, fetching
   * schema information from the Schema Registry for the specified schema. If the
   * SchemaRegistryClient passed in to the constructor is null, causes an exception to be thrown.
   * This method is thread-safe and can be called concurrently.
   *
   * @param table The TableId object used to identify the table,
   * @return The resulting BigQuery Table object.
   */
  public Table updateTable(TableId table) {
    logger.info("Attempting to update table {}", table);
    try {
      TableInfo tableInfo = getTableInfo(table);
      logger.trace("Sending table schema to BigQuery");
      return bigQuery.update(tableInfo);
    } catch (IOException | RestClientException err) {
      throw new SchemaRegistryConnectException("Failed to update table schema", err);
    }
  }

  private String getSchemaName(TableId table) {
    return tablesToTopics.get(table) + "-value";
  }

  private TableInfo getTableInfo(TableId table) throws IOException, RestClientException {
    String schema = getSchemaName(table);
    Schema bigQuerySchema = parseAndConvertAvroSchema(schema);
    TableDefinition tableDefinition = StandardTableDefinition.of(bigQuerySchema);
    return TableInfo.of(table, tableDefinition);
  }

  private Schema parseAndConvertAvroSchema(
      String schemaName) throws IOException, RestClientException {
    logger.trace("Fetching schema metadata for schema {}", schemaName);
    // getLatestSchemaMetadata() is already synchronized, don't have to worry about it
    SchemaMetadata latestSchemaMetadata =
        schemaRegistryClient.getLatestSchemaMetadata(schemaName);

    logger.trace("Parsing and converting schema from Avro string to BigQuery Schema");
    org.apache.avro.Schema avroSchema = new Parser().parse(latestSchemaMetadata.getSchema());
    org.apache.kafka.connect.data.Schema kafkaConnectSchema;
    // Synchronize to avoid issues with caching performed by the avroData object
    synchronized (avroData) {
      kafkaConnectSchema = avroData.toConnectSchema(avroSchema);
    }
    return schemaConverter.convertSchema(kafkaConnectSchema);
  }
}
