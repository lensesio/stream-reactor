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
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.write.batch.KCBQThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MergeQueriesTest {

  private static final String KEY = "kafkaKey";

  private static final int BATCH_NUMBER = 42;
  private static final TableId DESTINATION_TABLE = TableId.of("ds1", "t");
  private static final TableId INTERMEDIATE_TABLE = TableId.of("ds1", "t_tmp_6_uuid_epoch");
  private static final Schema INTERMEDIATE_TABLE_SCHEMA = constructIntermediateTable();

  @Mock private MergeBatches mergeBatches;
  @Mock private KCBQThreadPoolExecutor executor;
  @Mock private BigQuery bigQuery;
  @Mock private SchemaManager schemaManager;
  @Mock private SinkTaskContext context;

  @Before
  public void setUp() {
    when(schemaManager.cachedSchema(INTERMEDIATE_TABLE)).thenReturn(INTERMEDIATE_TABLE_SCHEMA);
  }

  private MergeQueries mergeQueries(boolean insertPartitionTime, boolean upsert, boolean delete) {
    return new MergeQueries(
        KEY, insertPartitionTime, upsert, delete, mergeBatches, executor, bigQuery, schemaManager, context
    );
  }

  private static Schema constructIntermediateTable() {
    List<Field> fields = new ArrayList<>();

    List<Field> valueFields = Arrays.asList(
        Field.of("f1", LegacySQLTypeName.STRING),
        Field.of("f2", LegacySQLTypeName.RECORD,
            Field.of("nested_f1", LegacySQLTypeName.INTEGER)
        ),
        Field.of("f3", LegacySQLTypeName.BOOLEAN),
        Field.of("f4", LegacySQLTypeName.BYTES)
    );
    Field wrappedValueField = Field
        .newBuilder(MergeQueries.INTERMEDIATE_TABLE_VALUE_FIELD_NAME, LegacySQLTypeName.RECORD, valueFields.toArray(new Field[0]))
        .setMode(Field.Mode.NULLABLE)
        .build();
    fields.add(wrappedValueField);

    List<Field> keyFields = Arrays.asList(
        Field.of("k1", LegacySQLTypeName.STRING),
        Field.of("k2", LegacySQLTypeName.RECORD,
            Field.of("nested_k1", LegacySQLTypeName.RECORD,
                Field.of("doubly_nested_k", LegacySQLTypeName.BOOLEAN)
            ),
            Field.of("nested_k2", LegacySQLTypeName.INTEGER)
        )
    );
    Field kafkaKeyField = Field.newBuilder(MergeQueries.INTERMEDIATE_TABLE_KEY_FIELD_NAME, LegacySQLTypeName.RECORD, keyFields.toArray(new Field[0]))
        .setMode(Field.Mode.REQUIRED)
        .build();
    fields.add(kafkaKeyField);

    Field partitionTimeField = Field
        .newBuilder(MergeQueries.INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME, LegacySQLTypeName.TIMESTAMP)
        .setMode(Field.Mode.NULLABLE)
        .build();
    fields.add(partitionTimeField);

    Field batchNumberField = Field
        .newBuilder(MergeQueries.INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD, LegacySQLTypeName.INTEGER)
        .setMode(Field.Mode.REQUIRED)
        .build();
    fields.add(batchNumberField);

    return Schema.of(fields);
  }

  @Test
  public void testUpsertQueryWithPartitionTime() {
    String expectedQuery =
        "MERGE " + table(DESTINATION_TABLE) + " dstTableAlias "
          + "USING (SELECT * FROM (SELECT ARRAY_AGG(x ORDER BY i DESC LIMIT 1)[OFFSET(0)] src "
            + "FROM " + table(INTERMEDIATE_TABLE) + " x "
            + "WHERE batchNumber=" + BATCH_NUMBER + " "
            + "GROUP BY key.k1, key.k2.nested_k1.doubly_nested_k, key.k2.nested_k2)) "
          + "ON dstTableAlias." + KEY + "=src.key "
          + "WHEN MATCHED "
            + "THEN UPDATE SET dstTableAlias.`f1`=src.value.f1, dstTableAlias.`f2`=src.value.f2, dstTableAlias.`f3`=src.value.f3, dstTableAlias.`f4`=src.value.f4 "
          + "WHEN NOT MATCHED "
            + "THEN INSERT (`"
              + KEY + "`, "
              + "_PARTITIONTIME, "
              + "`f1`, `f2`, `f3`, `f4`) "
            + "VALUES ("
              + "src.key, "
              + "CAST(CAST(DATE(src.partitionTime) AS DATE) AS TIMESTAMP), "
              + "src.value.f1, src.value.f2, src.value.f3, src.value.f4"
            + ");";
    String actualQuery = mergeQueries(true, true, false)
        .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);
    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testUpsertQueryWithoutPartitionTime() {
    String expectedQuery =
        "MERGE " + table(DESTINATION_TABLE) + " dstTableAlias "
          + "USING (SELECT * FROM (SELECT ARRAY_AGG(x ORDER BY i DESC LIMIT 1)[OFFSET(0)] src "
            + "FROM " + table(INTERMEDIATE_TABLE) + " x "
            + "WHERE batchNumber=" + BATCH_NUMBER + " "
            + "GROUP BY key.k1, key.k2.nested_k1.doubly_nested_k, key.k2.nested_k2)) "
          + "ON dstTableAlias." + KEY + "=src.key "
          + "WHEN MATCHED "
            + "THEN UPDATE SET dstTableAlias.`f1`=src.value.f1, dstTableAlias.`f2`=src.value.f2, dstTableAlias.`f3`=src.value.f3, dstTableAlias.`f4`=src.value.f4 "
          + "WHEN NOT MATCHED "
            + "THEN INSERT (`"
              + KEY + "`, "
              + "`f1`, `f2`, `f3`, `f4`) "
            + "VALUES ("
              + "src.key, "
              + "src.value.f1, src.value.f2, src.value.f3, src.value.f4"
            + ");";
    String actualQuery = mergeQueries(false, true, false)
        .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);
    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testDeleteQueryWithPartitionTime() {
    String expectedQuery =
        "MERGE " + table(DESTINATION_TABLE) + " "
          + "USING ("
            + "SELECT batch.key AS key, partitionTime, value "
              + "FROM ("
                + "SELECT src.i, src.key FROM ("
                  + "SELECT ARRAY_AGG("
                    + "x ORDER BY i DESC LIMIT 1"
                  + ")[OFFSET(0)] src "
                  + "FROM ("
                    + "SELECT * FROM " + table(INTERMEDIATE_TABLE) + " "
                      + "WHERE batchNumber=" + BATCH_NUMBER
                  + ") x "
                  + "WHERE x.value IS NULL "
                  + "GROUP BY key.k1, key.k2.nested_k1.doubly_nested_k, key.k2.nested_k2)) AS deletes "
                + "RIGHT JOIN ("
                  + "SELECT * FROM " + table(INTERMEDIATE_TABLE) + " "
                    + "WHERE batchNumber=" + BATCH_NUMBER
                + ") AS batch "
                + "USING (key) "
              + "WHERE deletes.i IS NULL OR batch.i >= deletes.i "
              + "ORDER BY batch.i ASC) AS src "
            + "ON `" + DESTINATION_TABLE.getTable() + "`." + KEY + "=src.key AND src.value IS NULL "
            + "WHEN MATCHED "
              + "THEN DELETE "
            + "WHEN NOT MATCHED AND src.value IS NOT NULL "
              + "THEN INSERT (`"
                + KEY + "`, "
                + "_PARTITIONTIME, "
                + "`f1`, `f2`, `f3`, `f4`) "
              + "VALUES ("
                + "src.key, "
                + "CAST(CAST(DATE(src.partitionTime) AS DATE) AS TIMESTAMP), "
                + "src.value.f1, src.value.f2, src.value.f3, src.value.f4"
            + ");";
    String actualQuery = mergeQueries(true, false, true)
        .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);
    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testDeleteQueryWithoutPartitionTime() {
    String expectedQuery =
        "MERGE " + table(DESTINATION_TABLE) + " "
          + "USING ("
            + "SELECT batch.key AS key, value "
              + "FROM ("
                + "SELECT src.i, src.key FROM ("
                  + "SELECT ARRAY_AGG("
                    + "x ORDER BY i DESC LIMIT 1"
                  + ")[OFFSET(0)] src "
                  + "FROM ("
                    + "SELECT * FROM " + table(INTERMEDIATE_TABLE) + " "
                      + "WHERE batchNumber=" + BATCH_NUMBER
                  + ") x "
                  + "WHERE x.value IS NULL "
                  + "GROUP BY key.k1, key.k2.nested_k1.doubly_nested_k, key.k2.nested_k2)) AS deletes "
                + "RIGHT JOIN ("
                  + "SELECT * FROM " + table(INTERMEDIATE_TABLE) + " "
                    + "WHERE batchNumber=" + BATCH_NUMBER
                + ") AS batch "
                + "USING (key) "
              + "WHERE deletes.i IS NULL OR batch.i >= deletes.i "
              + "ORDER BY batch.i ASC) AS src "
            + "ON `" + DESTINATION_TABLE.getTable() + "`." + KEY + "=src.key AND src.value IS NULL "
            + "WHEN MATCHED "
              + "THEN DELETE "
            + "WHEN NOT MATCHED AND src.value IS NOT NULL "
              + "THEN INSERT (`"
                + KEY + "`, "
                + "`f1`, `f2`, `f3`, `f4`) "
              + "VALUES ("
                + "src.key, "
                + "src.value.f1, src.value.f2, src.value.f3, src.value.f4"
            + ");";
    String actualQuery = mergeQueries(false, false, true)
        .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);
    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testUpsertDeleteQueryWithPartitionTime() {
    String expectedQuery =
        "MERGE " + table(DESTINATION_TABLE) + " dstTableAlias "
          + "USING (SELECT * FROM (SELECT ARRAY_AGG(x ORDER BY i DESC LIMIT 1)[OFFSET(0)] src "
            + "FROM " + table(INTERMEDIATE_TABLE) + " x "
            + "WHERE batchNumber=" + BATCH_NUMBER + " "
            + "GROUP BY key.k1, key.k2.nested_k1.doubly_nested_k, key.k2.nested_k2)) "
          + "ON dstTableAlias." + KEY + "=src.key "
          + "WHEN MATCHED AND src.value IS NOT NULL "
            + "THEN UPDATE SET dstTableAlias.`f1`=src.value.f1, dstTableAlias.`f2`=src.value.f2, dstTableAlias.`f3`=src.value.f3, dstTableAlias.`f4`=src.value.f4 "
          + "WHEN MATCHED AND src.value IS NULL "
            + "THEN DELETE "
          + "WHEN NOT MATCHED AND src.value IS NOT NULL "
            + "THEN INSERT (`"
              + KEY + "`, "
              + "_PARTITIONTIME, "
              + "`f1`, `f2`, `f3`, `f4`) "
            + "VALUES ("
              + "src.key, "
              + "CAST(CAST(DATE(src.partitionTime) AS DATE) AS TIMESTAMP), "
              + "src.value.f1, src.value.f2, src.value.f3, src.value.f4"
            + ");";
    String actualQuery = mergeQueries(true, true, true)
        .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);
    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testUpsertDeleteQueryWithoutPartitionTime() {
    String expectedQuery =
        "MERGE " + table(DESTINATION_TABLE) + " dstTableAlias "
          + "USING (SELECT * FROM (SELECT ARRAY_AGG(x ORDER BY i DESC LIMIT 1)[OFFSET(0)] src "
            + "FROM " + table(INTERMEDIATE_TABLE) + " x "
            + "WHERE batchNumber=" + BATCH_NUMBER + " "
            + "GROUP BY key.k1, key.k2.nested_k1.doubly_nested_k, key.k2.nested_k2)) "
          + "ON dstTableAlias." + KEY + "=src.key "
          + "WHEN MATCHED AND src.value IS NOT NULL "
            + "THEN UPDATE SET dstTableAlias.`f1`=src.value.f1, dstTableAlias.`f2`=src.value.f2, dstTableAlias.`f3`=src.value.f3, dstTableAlias.`f4`=src.value.f4 "
          + "WHEN MATCHED AND src.value IS NULL "
            + "THEN DELETE "
          + "WHEN NOT MATCHED AND src.value IS NOT NULL "
            + "THEN INSERT (`"
              + KEY + "`, "
              + "`f1`, `f2`, `f3`, `f4`) "
            + "VALUES ("
              + "src.key, "
              + "src.value.f1, src.value.f2, src.value.f3, src.value.f4"
            + ");";    String actualQuery = mergeQueries(false, true, true)
        .mergeFlushQuery(INTERMEDIATE_TABLE, DESTINATION_TABLE, BATCH_NUMBER);
    assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testBatchClearQuery() {
    String expectedQuery =
        "DELETE FROM " + table(INTERMEDIATE_TABLE)
          + " WHERE batchNumber <= " + BATCH_NUMBER
          + " AND _PARTITIONTIME IS NOT NULL;";
    // No difference in batch clearing between upsert, delete, and both, or with or without partition time
    String actualQuery = MergeQueries.batchClearQuery(INTERMEDIATE_TABLE, BATCH_NUMBER);
    assertEquals(expectedQuery, actualQuery);
  }

  private String table(TableId table) {
    return String.format("`%s`.`%s`", table.getDataset(), table.getTable());
  }
}
