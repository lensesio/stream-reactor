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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.exception.ExpectedInterruptException;
import com.wepay.kafka.connect.bigquery.write.batch.KCBQThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.wepay.kafka.connect.bigquery.utils.TableNameUtils.destTable;
import static com.wepay.kafka.connect.bigquery.utils.TableNameUtils.intTable;

public class MergeQueries {
  public static final String INTERMEDIATE_TABLE_KEY_FIELD_NAME = "key";
  public static final String INTERMEDIATE_TABLE_VALUE_FIELD_NAME = "value";
  public static final String INTERMEDIATE_TABLE_ITERATION_FIELD_NAME = "i";
  public static final String INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME = "partitionTime";
  public static final String INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD = "batchNumber";
  public static final String DESTINATION_TABLE_ALIAS = "dstTableAlias";

  private static final Logger logger = LoggerFactory.getLogger(MergeQueries.class);

  private final String keyFieldName;
  private final boolean insertPartitionTime;
  private final boolean upsertEnabled;
  private final boolean deleteEnabled;
  private final MergeBatches mergeBatches;
  private final KCBQThreadPoolExecutor executor;
  private final BigQuery bigQuery;
  private final SchemaManager schemaManager;
  private final SinkTaskContext context;

  public MergeQueries(BigQuerySinkTaskConfig config,
                      MergeBatches mergeBatches,
                      KCBQThreadPoolExecutor executor,
                      BigQuery bigQuery,
                      SchemaManager schemaManager,
                      SinkTaskContext context) {
    this(
      config.getKafkaKeyFieldName().orElseThrow(() ->
          new ConnectException("Kafka key field must be configured when upsert/delete is enabled")
      ),
      config.getBoolean(config.BIGQUERY_PARTITION_DECORATOR_CONFIG),
      config.getBoolean(config.UPSERT_ENABLED_CONFIG),
      config.getBoolean(config.DELETE_ENABLED_CONFIG),
      mergeBatches,
      executor,
      bigQuery,
      schemaManager,
      context
    );
  }

  @VisibleForTesting
  MergeQueries(String keyFieldName,
               boolean insertPartitionTime,
               boolean upsertEnabled,
               boolean deleteEnabled,
               MergeBatches mergeBatches,
               KCBQThreadPoolExecutor executor,
               BigQuery bigQuery,
               SchemaManager schemaManager,
               SinkTaskContext context) {
    this.keyFieldName = keyFieldName;
    this.insertPartitionTime = insertPartitionTime;
    this.upsertEnabled = upsertEnabled;
    this.deleteEnabled = deleteEnabled;
    this.mergeBatches = mergeBatches;
    this.executor = executor;
    this.bigQuery = bigQuery;
    this.schemaManager = schemaManager;
    this.context = context;
  }

  public void mergeFlushAll() {
    logger.debug("Triggering merge flush for all tables");
    mergeBatches.intermediateTables().forEach(this::mergeFlush);
  }

  public void mergeFlush(TableId intermediateTable) {
    final TableId destinationTable = mergeBatches.destinationTableFor(intermediateTable);
    final int batchNumber = mergeBatches.incrementBatch(intermediateTable);
    logger.trace("Triggering merge flush from {} to {} for batch {}",
        intTable(intermediateTable), destTable(destinationTable), batchNumber);

    executor.execute(() -> {
      try {
        mergeFlush(intermediateTable, destinationTable, batchNumber);
      } catch (InterruptedException e) {
        throw new ExpectedInterruptException(String.format(
            "Interrupted while performing merge flush of batch %d from %s to %s",
            batchNumber, intTable(intermediateTable), destTable(destinationTable)));
      }
    });
  }

  private void mergeFlush(
      TableId intermediateTable, TableId destinationTable, int batchNumber
  ) throws InterruptedException{
    // If there are rows to flush in this batch, flush them
    if (mergeBatches.prepareToFlush(intermediateTable, batchNumber)) {
      logger.debug("Running merge query on batch {} from {}",
          batchNumber, intTable(intermediateTable));
      String mergeFlushQuery = mergeFlushQuery(intermediateTable, destinationTable, batchNumber);
      logger.trace(mergeFlushQuery);
      bigQuery.query(QueryJobConfiguration.of(mergeFlushQuery));
      logger.trace("Merge from {} to {} completed",
          intTable(intermediateTable), destTable(destinationTable));

      logger.debug("Recording flush success for batch {} from {}",
          batchNumber, intTable(intermediateTable));
      mergeBatches.recordSuccessfulFlush(intermediateTable, batchNumber);

      // Commit those offsets ASAP
      context.requestCommit();

      logger.info("Completed merge flush of batch {} from {} to {}",
          batchNumber, intTable(intermediateTable), destTable(destinationTable));
    }

    // After, regardless of whether we flushed or not, clean up old batches from the intermediate
    // table. Some rows may be several batches old but still in the table if they were in the
    // streaming buffer during the last purge.
    logger.trace("Clearing batches from {} on back from {}", batchNumber, intTable(intermediateTable));
    String batchClearQuery = batchClearQuery(intermediateTable, batchNumber);
    logger.trace(batchClearQuery);
    bigQuery.query(QueryJobConfiguration.of(batchClearQuery));
  }

  @VisibleForTesting
  String mergeFlushQuery(TableId intermediateTable, TableId destinationTable, int batchNumber) {
    Schema intermediateSchema = schemaManager.cachedSchema(intermediateTable);

    if (upsertEnabled && deleteEnabled) {
      return upsertDeleteMergeFlushQuery(intermediateTable, destinationTable, batchNumber, intermediateSchema);
    } else if (upsertEnabled) {
      return upsertMergeFlushQuery(intermediateTable, destinationTable, batchNumber, intermediateSchema);
    } else if (deleteEnabled) {
      return deleteMergeFlushQuery(intermediateTable, destinationTable, batchNumber, intermediateSchema);
    } else {
      throw new IllegalStateException("At least one of upsert or delete must be enabled for merge flushing to occur.");
    }
  }

  /*
      MERGE `<dataset>`.`<destinationTable>`
      USING (
        SELECT * FROM (
          SELECT ARRAY_AGG(
            x ORDER BY i DESC LIMIT 1
          )[OFFSET(0)] src
          FROM `<dataset>`.`<intermediateTable>` x
          WHERE batchNumber=<batchNumber>
          GROUP BY key.<field>[, key.<field>...]
        )
      )
      ON `<destinationTable>`.<keyField>=src.key
      WHEN MATCHED AND src.value IS NOT NULL
        THEN UPDATE SET `<valueField>`=src.value.<field>[, `<valueField>`=src.value.<field>...]
      WHEN MATCHED AND src.value IS NULL
        THEN DELETE
      WHEN NOT MATCHED AND src.value IS NOT NULL
        THEN INSERT (`<keyField>`, [_PARTITIONTIME, ]`<valueField>`[, `<valueField>`])
        VALUES (
          src.key,
          [CAST(CAST(DATE(src.partitionTime) AS DATE) AS TIMESTAMP),]
          src.value.<field>[, src.value.<field>...]
        );
   */
  private String upsertDeleteMergeFlushQuery(
      TableId intermediateTable, TableId destinationTable, int batchNumber, Schema intermediateSchema
  ) {
    List<String> keyFields = listFields(
        intermediateSchema.getFields().get(INTERMEDIATE_TABLE_KEY_FIELD_NAME).getSubFields(),
        INTERMEDIATE_TABLE_KEY_FIELD_NAME + "."
    );

    List<String> valueColumns = valueColumns(intermediateSchema);

    final String key = INTERMEDIATE_TABLE_KEY_FIELD_NAME;
    final String i = INTERMEDIATE_TABLE_ITERATION_FIELD_NAME;
    final String value = INTERMEDIATE_TABLE_VALUE_FIELD_NAME;
    final String batch = INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD;

    return "MERGE " + table(destinationTable) + " " + DESTINATION_TABLE_ALIAS + " "
        + "USING ("
          + "SELECT * FROM ("
            + "SELECT ARRAY_AGG("
              + "x ORDER BY " + i + " DESC LIMIT 1"
            + ")[OFFSET(0)] src "
            + "FROM " + table(intermediateTable) + " x "
            + "WHERE " + batch + "=" + batchNumber + " "
            + "GROUP BY " + String.join(", ", keyFields)
          + ")"
        + ") "
        + "ON " + DESTINATION_TABLE_ALIAS + "." + keyFieldName + "=src." + key + " "
        + "WHEN MATCHED AND src." + value + " IS NOT NULL "
          + "THEN UPDATE SET " + valueColumns.stream().map(col -> DESTINATION_TABLE_ALIAS + ".`" + col + "`=src." + value + "." + col).collect(Collectors.joining(", ")) + " "
        + "WHEN MATCHED AND src." + value + " IS NULL "
          + "THEN DELETE "
        + "WHEN NOT MATCHED AND src." + value + " IS NOT NULL "
          + "THEN INSERT (`"
            + keyFieldName + "`, "
            + partitionTimePseudoColumn()
            + "`"
            + String.join("`, `", valueColumns) + "`) "
          + "VALUES ("
            + "src." + key + ", "
            + partitionTimeValue()
            + valueColumns.stream().map(col -> "src." + value + "." + col).collect(Collectors.joining(", "))
        + ");";
  }

  /*
      MERGE `<dataset>`.`<destinationTable>`
      USING (
        SELECT * FROM (
          SELECT ARRAY_AGG(
            x ORDER BY i DESC LIMIT 1
          )[OFFSET(0)] src
          FROM `<dataset>`.`<intermediateTable>` x
          WHERE batchNumber=<batchNumber>
          GROUP BY key.<field>[, key.<field>...]
        )
      )
      ON `<destinationTable>`.<keyField>=src.key
      WHEN MATCHED
        THEN UPDATE SET `<valueField>`=src.value.<field>[, `<valueField>`=src.value.<field>...]
      WHEN NOT MATCHED
        THEN INSERT (`<keyField>`, [_PARTITIONTIME, ]`<valueField>`[, `<valueField>`])
        VALUES (
          src.key,
          [CAST(CAST(DATE(src.partitionTime) AS DATE) AS TIMESTAMP),]
          src.value.<field>[, src.value.<field>...]
        );
   */
  private String upsertMergeFlushQuery(
      TableId intermediateTable, TableId destinationTable, int batchNumber, Schema intermediateSchema
  ) {
    List<String> keyFields = listFields(
        intermediateSchema.getFields().get(INTERMEDIATE_TABLE_KEY_FIELD_NAME).getSubFields(),
        INTERMEDIATE_TABLE_KEY_FIELD_NAME + "."
    );

    List<String> valueColumns = valueColumns(intermediateSchema);

    final String key = INTERMEDIATE_TABLE_KEY_FIELD_NAME;
    final String i = INTERMEDIATE_TABLE_ITERATION_FIELD_NAME;
    final String value = INTERMEDIATE_TABLE_VALUE_FIELD_NAME;
    final String batch = INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD;

    return "MERGE " + table(destinationTable) + " " + DESTINATION_TABLE_ALIAS + " "
        + "USING ("
          + "SELECT * FROM ("
            + "SELECT ARRAY_AGG("
              + "x ORDER BY " + i + " DESC LIMIT 1"
            + ")[OFFSET(0)] src "
            + "FROM " + table(intermediateTable) + " x "
            + "WHERE " + batch + "=" + batchNumber + " "
            + "GROUP BY " + String.join(", ", keyFields)
          + ")"
        + ") "
        + "ON " + DESTINATION_TABLE_ALIAS + "." + keyFieldName + "=src." + key + " "
        + "WHEN MATCHED "
          + "THEN UPDATE SET " + valueColumns.stream().map(col -> DESTINATION_TABLE_ALIAS + ".`" + col + "`=src." + value + "." + col).collect(Collectors.joining(", ")) + " "
        + "WHEN NOT MATCHED "
          + "THEN INSERT (`"
            + keyFieldName + "`, "
            + partitionTimePseudoColumn()
            + "`"
            + String.join("`, `", valueColumns) + "`) "
          + "VALUES ("
            + "src." + key + ", "
            + partitionTimeValue()
            + valueColumns.stream().map(col -> "src." + value + "." + col).collect(Collectors.joining(", "))
          + ");";
  }

  /*
        Delete-only is the trickiest mode. Naively, we could just run a MERGE using the intermediate
      table as a source and sort in ascending order of iteration. However, this would miss an edge
      case where, for a given key, a non-tombstone record is sent and then followed by a tombstone,
      and would result in all rows with that key being deleted from the table, followed by an
      insertion of a row for the initial non-tombstone record. This is incorrect; any and all
      records with a given key that precede a tombstone should either never make it into BigQuery or
      be deleted once the tombstone record is merge flushed.
        So instead, we have to try to filter out rows from the source (i.e., intermediate) table
      that precede tombstone records for their keys. We do this by:
        - Finding the latest tombstone row for each key in the current batch and extracting the
          iteration number for each, referring to this as the "deletes" table
        - Joining that with the current batch from the intermediate table on the row key, keeping
          both tables' iteration numbers (a RIGHT JOIN is used so that rows whose keys don't have
          any tombstones present are included with a NULL iteration number for the "deletes" table)
        - Filtering out all rows where the "delete" table's iteration number is non-null, and their
          iteration number is less than the "delete" table's iteration number
        This gives us only rows from the most recent tombstone onward, and works in both cases where
      the most recent row for a key is or is not a tombstone.

      MERGE `<dataset>`.`<destinationTable>`
      USING (
        SELECT batch.key AS key, [partitionTime, ]value
         FROM (
          SELECT src.i, src.key FROM (
            SELECT ARRAY_AGG(
              x ORDER BY i DESC LIMIT 1
            )[OFFSET(0)] src
            FROM (
              SELECT * FROM `<dataset>`.`<intermediateTable>`
              WHERE batchNumber=<batchNumber>
            ) x
            WHERE x.value IS NULL
            GROUP BY key.<field>[, key.<field>...])) AS deletes
          RIGHT JOIN (
            SELECT * FROM `<dataset>`.`<intermediateTable`
            WHERE batchNumber=<batchNumber>
          ) AS batch
          USING (key)
        WHERE deletes.i IS NULL OR batch.i >= deletes.i
        ORDER BY batch.i ASC) AS src
      ON `<destinationTable>`.<keyField>=src.key AND src.value IS NULL
      WHEN MATCHED
        THEN DELETE
      WHEN NOT MATCHED AND src.value IS NOT NULL
      THEN INSERT (`<keyField>`, [_PARTITIONTIME, ]`<valueField>`[, `<valueField>`])
      VALUES (
        src.key,
        [CAST(CAST(DATE(src.partitionTime) AS DATE) AS TIMESTAMP),]
        src.value.<field>[, src.value.<field>...]
      );
   */
  private String deleteMergeFlushQuery(
      TableId intermediateTable, TableId destinationTable, int batchNumber, Schema intermediateSchema
  ) {
    List<String> keyFields = listFields(
        intermediateSchema.getFields().get(INTERMEDIATE_TABLE_KEY_FIELD_NAME).getSubFields(),
        INTERMEDIATE_TABLE_KEY_FIELD_NAME + "."
    );

    List<String> valueColumns = valueColumns(intermediateSchema);

    final String key = INTERMEDIATE_TABLE_KEY_FIELD_NAME;
    final String i = INTERMEDIATE_TABLE_ITERATION_FIELD_NAME;
    final String value = INTERMEDIATE_TABLE_VALUE_FIELD_NAME;
    final String batch = INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD;

    return "MERGE " + table(destinationTable) + " "
        + "USING ("
          + "SELECT batch." + key + " AS " + key + ", " + partitionTimeColumn() + value + " "
            + "FROM ("
              + "SELECT src." + i + ", src." + key + " FROM ("
                + "SELECT ARRAY_AGG("
                  + "x ORDER BY " + i + " DESC LIMIT 1"
                + ")[OFFSET(0)] src "
                + "FROM ("
                  + "SELECT * FROM " + table(intermediateTable) + " "
                  + "WHERE " + batch + "=" + batchNumber
                + ") x "
                + "WHERE x." + value + " IS NULL "
                + "GROUP BY " + String.join(", ", keyFields) + ")) AS deletes "
            + "RIGHT JOIN ("
              + "SELECT * FROM " + table(intermediateTable) + " "
              + "WHERE " + batch + "=" + batchNumber
            + ") AS batch "
            + "USING (" + key + ") "
          + "WHERE deletes." + i + " IS NULL OR batch." + i + " >= deletes." + i + " "
          + "ORDER BY batch." + i + " ASC) AS src "
        + "ON `" + destinationTable.getTable() + "`." + keyFieldName + "=src." + key + " AND src." + value + " IS NULL "
        + "WHEN MATCHED "
          + "THEN DELETE "
        + "WHEN NOT MATCHED AND src." + value + " IS NOT NULL "
          + "THEN INSERT (`"
            + keyFieldName + "`, "
            + partitionTimePseudoColumn()
            + "`"
            + String.join("`, `", valueColumns) + "`) "
          + "VALUES ("
            + "src." + key + ", "
            + partitionTimeValue()
            + valueColumns.stream().map(col -> "src." + value + "." + col).collect(Collectors.joining(", "))
          + ");";
  }

  private String table(TableId tableId) {
    return String.format("`%s`.`%s`", tableId.getDataset(), tableId.getTable());
  }

  private List<String> valueColumns(Schema intermediateTableSchema) {
    return intermediateTableSchema.getFields().get(INTERMEDIATE_TABLE_VALUE_FIELD_NAME).getSubFields()
        .stream()
        .map(Field::getName)
        .collect(Collectors.toList());
  }

  private String partitionTimePseudoColumn() {
    return insertPartitionTime ? "_PARTITIONTIME, " : "";
  }

  private String partitionTimeValue() {
    return insertPartitionTime
        ? "CAST(CAST(DATE(src." + INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME + ") AS DATE) AS TIMESTAMP), "
        : "";
  }

  private String partitionTimeColumn() {
    return insertPartitionTime
        ? INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME + ", "
        : "";
  }

  // DELETE FROM `<dataset>`.`<intermediateTable>` WHERE batchNumber <= <batchNumber> AND _PARTITIONTIME IS NOT NULL;
  @VisibleForTesting
  static String batchClearQuery(TableId intermediateTable, int batchNumber) {
    return new StringBuilder("DELETE FROM `").append(intermediateTable.getDataset()).append("`.`").append(intermediateTable.getTable()).append("` ")
        .append("WHERE ")
          .append(INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD).append(" <= ").append(batchNumber).append(" ")
          // Use this clause to filter out rows that are still in the streaming buffer, which should
          // not be subjected to UPDATE or DELETE operations or the query will FAIL
          .append("AND _PARTITIONTIME IS NOT NULL")
        .append(";")
        .toString();
  }

  private static List<String> listFields(FieldList keyFields, String prefix) {
    return keyFields.stream()
        .flatMap(field -> {
          String fieldName = prefix + field.getName();
          FieldList subFields = field.getSubFields();
          if (subFields == null) {
            return Stream.of(fieldName);
          }
          return listFields(subFields, fieldName + ".").stream();
        }).collect(Collectors.toList());
  }
}
