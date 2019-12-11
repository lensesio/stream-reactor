package com.wepay.kafka.connect.bigquery;


import com.google.cloud.bigquery.*;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.KafkaDataBuilder;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for managing Schemas of BigQuery tables (creating and updating).
 */
public class SchemaManager {
  private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);
  private static final String VALUE = "value";
  private static final String KEY = "key";

  private final SchemaRetriever schemaRetriever;
  private final SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter;
  private final BigQuery bigQuery;
  private final boolean includeKafkaKey;
  private final boolean includeKafkaData;
  private final String kafkaKeyFieldName;
  private final String kafkaDataFieldName;

  /**
   * @param schemaRetriever Used to determine the Kafka Connect Schema that should be used for a
   *                        given table.
   * @param schemaConverter Used to convert Kafka Connect Schemas into BigQuery format.
   * @param bigQuery Used to communicate create/update requests to BigQuery.
   */
  public SchemaManager(
      SchemaRetriever schemaRetriever,
      SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter,
      BigQuery bigQuery,
      boolean includeKafkaKey,
      boolean includeKafkaData,
      String kafkaKeyFieldName,
      String kafkaDataFieldName) {
    this.schemaRetriever = schemaRetriever;
    this.schemaConverter = schemaConverter;
    this.bigQuery = bigQuery;
    this.includeKafkaKey = includeKafkaKey;
    this.includeKafkaData = includeKafkaData;
    this.kafkaKeyFieldName = kafkaKeyFieldName;
    this.kafkaDataFieldName = kafkaDataFieldName;
  }

  /**
   * Create a new table in BigQuery.
   * @param table The BigQuery table to create.
   * @param topic The Kafka topic used to determine the schema.
   */
  public void createTable(TableId table, String topic) {
    Schema kafkaValueSchema = schemaRetriever.retrieveSchema(table, topic, VALUE);
    Schema kafkaKeySchema = includeKafkaKey ? schemaRetriever.retrieveSchema(table, topic, KEY) : null;
    bigQuery.create(constructTableInfo(table, kafkaKeySchema, kafkaValueSchema));
  }

  /**
   * Update an existing table in BigQuery.
   * @param table The BigQuery table to update.
   * @param topic The Kafka topic used to determine the schema.
   */
  public void updateSchema(TableId table, String topic) {
    Schema kafkaValueSchema = schemaRetriever.retrieveSchema(table, topic, VALUE);
    Schema kafkaKeySchema = includeKafkaKey ? schemaRetriever.retrieveSchema(table, topic, KEY) : null;
    TableInfo tableInfo = constructTableInfo(table, kafkaKeySchema, kafkaValueSchema);
    logger.info("Attempting to update table `{}` with schema {}",
        table, tableInfo.getDefinition().getSchema());
    bigQuery.update(tableInfo);
  }

  // package private for testing.
  TableInfo constructTableInfo(TableId table, Schema kafkaKeySchema, Schema kafkaValueSchema) {
    com.google.cloud.bigquery.Schema bigQuerySchema = getBigQuerySchema(kafkaKeySchema, kafkaValueSchema);
    StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
        .setSchema(bigQuerySchema)
        .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
        .build();
    TableInfo.Builder tableInfoBuilder =
        TableInfo.newBuilder(table, tableDefinition);
    if (kafkaValueSchema.doc() != null) {
      tableInfoBuilder.setDescription(kafkaValueSchema.doc());
    }
    return tableInfoBuilder.build();
  }

  private com.google.cloud.bigquery.Schema getBigQuerySchema(Schema kafkaKeySchema, Schema kafkaValueSchema) {
      List<Field> allFields = new ArrayList<> ();
      com.google.cloud.bigquery.Schema valueSchema = schemaConverter.convertSchema(kafkaValueSchema);
      allFields.addAll(valueSchema.getFields());
      if (includeKafkaKey) {
          com.google.cloud.bigquery.Schema keySchema = schemaConverter.convertSchema(kafkaKeySchema);
          Field kafkaKeyField = Field.newBuilder(kafkaKeyFieldName, LegacySQLTypeName.RECORD, keySchema.getFields())
                  .setMode(Field.Mode.NULLABLE).build();
          allFields.add(kafkaKeyField);
      }
      if (includeKafkaData) {
          Field kafkaDataField = KafkaDataBuilder.buildKafkaDataField(kafkaDataFieldName);
          allFields.add(kafkaDataField);
      }
      return com.google.cloud.bigquery.Schema.of(allFields);
  }

}
