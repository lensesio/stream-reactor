package com.wepay.kafka.connect.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import org.apache.kafka.connect.data.Schema;

/**
 * Class for managing Schemas of BigQuery tables (creating and updating).
 */
public class SchemaManager {
  private final SchemaRetriever schemaRetriever;
  private final SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter;
  private final BigQuery bigQuery;

  /**
   * @param schemaRetriever Used to determine the Kafka Connect Schema that should be used for a
   *                        given table.
   * @param schemaConverter Used to convert Kafka Connect Schemas into BigQuery format.
   * @param bigQuery Used to communicate create/update requests to BigQuery.
   */
  public SchemaManager(
      SchemaRetriever schemaRetriever,
      SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter,
      BigQuery bigQuery) {
    this.schemaRetriever = schemaRetriever;
    this.schemaConverter = schemaConverter;
    this.bigQuery = bigQuery;
  }

  /**
   * Create a new table in BigQuery.
   * @param table The BigQuery table to create.
   * @param topic The Kafka topic used to determine the schema.
   */
  public void createTable(TableId table, String topic) {
    Schema kafkaConnectSchema = schemaRetriever.retrieveSchema(table, topic);
    bigQuery.create(constructTableInfo(table, kafkaConnectSchema));
  }

  /**
   * Update an existing table in BigQuery.
   * @param table The BigQuery table to update.
   * @param topic The Kafka topic used to determine the schema.
   */
  public void updateSchema(TableId table, String topic) {
    Schema kafkaConnectSchema = schemaRetriever.retrieveSchema(table, topic);
    bigQuery.update(constructTableInfo(table, kafkaConnectSchema));
  }

  // package private for testing.
  TableInfo constructTableInfo(TableId table, Schema kafkaConnectSchema) {
    com.google.cloud.bigquery.Schema bigQuerySchema =
        schemaConverter.convertSchema(kafkaConnectSchema);
    StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
        .setSchema(bigQuerySchema)
        .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
        .build();
    TableInfo.Builder tableInfoBuilder =
        TableInfo.newBuilder(table, tableDefinition);
    if (kafkaConnectSchema.doc() != null) {
      tableInfoBuilder.setDescription(kafkaConnectSchema.doc());
    }
    return tableInfoBuilder.build();
  }
}
