/*
 * Copyright 2017-2020 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.write.batch.KCBQThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MergeQueriesTest {

  private static final String KEY = "kafkaKey";

  private static final int BATCH_NUMBER = 42;
  private static final int BIGQUERY_RETRY = 3;
  private static final int BIGQUERY_RETRY_WAIT = 1000;
  private static final TableId DESTINATION_TABLE = TableId.of("ds1", "t");
  private static final TableId INTERMEDIATE_TABLE = TableId.of("ds1", "t_tmp_6_uuid_epoch");
  private static final Schema INTERMEDIATE_TABLE_SCHEMA = constructIntermediateTable();

  private static final SinkRecord TEST_SINK_RECORD = new SinkRecord("test", 0, null, null, null, null, 0);
  @Mock
  private MergeBatches mergeBatches;
  @Mock
  private KCBQThreadPoolExecutor executor;
  @Mock
  private BigQuery bigQuery;
  @Mock
  private SchemaManager schemaManager;
  @Mock
  private SinkTaskContext context;

  @Before
  public void setUp() {
    when(schemaManager.cachedSchema(INTERMEDIATE_TABLE)).thenReturn(INTERMEDIATE_TABLE_SCHEMA);
  }

  private MergeQueries mergeQueries(boolean insertPartitionTime, boolean upsert, boolean delete) {
    return new MergeQueries(
        KEY, insertPartitionTime, upsert, delete, BIGQUERY_RETRY, BIGQUERY_RETRY_WAIT, mergeBatches, executor, bigQuery,
        schemaManager, context
    );
  }

  private void initialiseMergeBatches() {
    mergeBatches = new MergeBatches("_tmp_6_uuid_epoch");
    mergeBatches.intermediateTableFor(DESTINATION_TABLE);
  }

  private static Schema constructIntermediateTable() {
    List<Field> fields = new ArrayList<>();

    List<Field> valueFields =
        Arrays.asList(
            Field.of("f1", LegacySQLTypeName.STRING),
            Field.of("f2", LegacySQLTypeName.RECORD,
                Field.of("nested_f1", LegacySQLTypeName.INTEGER)
            ),
            Field.of("f3", LegacySQLTypeName.BOOLEAN),
            Field.of("f4", LegacySQLTypeName.BYTES)
        );
    Field wrappedValueField =
        Field
            .newBuilder(MergeQueries.INTERMEDIATE_TABLE_VALUE_FIELD_NAME, LegacySQLTypeName.RECORD, valueFields.toArray(
                new Field[0]))
            .setMode(Field.Mode.NULLABLE)
            .build();
    fields.add(wrappedValueField);

    List<Field> keyFields =
        Arrays.asList(
            Field.of("k1", LegacySQLTypeName.STRING),
            Field.of("k2", LegacySQLTypeName.RECORD,
                Field.of("nested_k1", LegacySQLTypeName.RECORD,
                    Field.of("doubly_nested_k", LegacySQLTypeName.BOOLEAN)
                ),
                Field.of("nested_k2", LegacySQLTypeName.INTEGER)
            )
        );
    Field kafkaKeyField =
        Field.newBuilder(MergeQueries.INTERMEDIATE_TABLE_KEY_FIELD_NAME, LegacySQLTypeName.RECORD, keyFields.toArray(
            new Field[0]))
            .setMode(Field.Mode.REQUIRED)
            .build();
    fields.add(kafkaKeyField);

    Field partitionTimeField =
        Field
            .newBuilder(MergeQueries.INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME, LegacySQLTypeName.TIMESTAMP)
            .setMode(Field.Mode.NULLABLE)
            .build();
    fields.add(partitionTimeField);

    Field batchNumberField =
        Field
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
    String actualQuery =
        mergeQueries(true, true, false)
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
    String actualQuery =
        mergeQueries(false, true, false)
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
    String actualQuery =
        mergeQueries(true, false, true)
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
    String actualQuery =
        mergeQueries(false, false, true)
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
    String actualQuery =
        mergeQueries(true, true, true)
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
            + ");";
    String actualQuery =
        mergeQueries(false, true, true)
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

  @Test
  public void testNoEmptyBatchCreation() {
    initialiseMergeBatches();

    mergeQueries(false, true, true).mergeFlush(INTERMEDIATE_TABLE);

    assertEquals(0, mergeBatches.incrementBatch(INTERMEDIATE_TABLE));
  }

  @Test
  public void testBatchCreation() {
    initialiseMergeBatches();

    mergeBatches.addToBatch(TEST_SINK_RECORD, INTERMEDIATE_TABLE, new HashMap<>());
    mergeQueries(false, true, true).mergeFlush(INTERMEDIATE_TABLE);

    assertEquals(1, mergeBatches.incrementBatch(INTERMEDIATE_TABLE));
  }

  @Test
  public void testBigQueryJobInternalErrorRetry() throws InterruptedException {
    // Arrange
    mergeBatches.addToBatch(TEST_SINK_RECORD, INTERMEDIATE_TABLE, new HashMap<>());

    TableResult tableResultReponse = mock(TableResult.class);
    BigQueryError jobInternalError =
        new BigQueryError("jobInternalError", null,
            "The job encountered an internal error during execution and was unable to complete successfully.");
    when(bigQuery.query(any()))
        .thenThrow(new BigQueryException(400, "mock job internal error", jobInternalError))
        .thenReturn(tableResultReponse);
    when(mergeBatches.destinationTableFor(INTERMEDIATE_TABLE)).thenReturn(DESTINATION_TABLE);
    when(mergeBatches.incrementBatch(INTERMEDIATE_TABLE)).thenReturn(0);
    when(mergeBatches.prepareToFlush(INTERMEDIATE_TABLE, 0)).thenReturn(true);
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(invocation -> {
      Runnable runnable = invocation.getArgument(0);
      runnable.run();
      latch.countDown();
      return null;
    }).when(executor).execute(any());
    MergeQueries mergeQueries = spy(mergeQueries(false, true, true));

    // Act
    mergeQueries.mergeFlush(INTERMEDIATE_TABLE);

    // Assert
    latch.await();
    verify(bigQuery, times(3)).query(any());
  }

  @Test
  public void testBigQueryInvalidQueryErrorRetry() throws InterruptedException {
    // Arrange
    mergeBatches.addToBatch(TEST_SINK_RECORD, INTERMEDIATE_TABLE, new HashMap<>());

    TableResult tableResultReponse = mock(TableResult.class);
    BigQueryError jobInternalError =
        new BigQueryError("invalidQuery", null,
            "Could not serialize access to table my_table due to concurrent update");
    when(bigQuery.query(any()))
        .thenThrow(new BigQueryException(400, "mock invalid query", jobInternalError))
        .thenReturn(tableResultReponse);
    when(mergeBatches.destinationTableFor(INTERMEDIATE_TABLE)).thenReturn(DESTINATION_TABLE);
    when(mergeBatches.incrementBatch(INTERMEDIATE_TABLE)).thenReturn(0);
    when(mergeBatches.prepareToFlush(INTERMEDIATE_TABLE, 0)).thenReturn(true);
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(invocation -> {
      Runnable runnable = invocation.getArgument(0);
      runnable.run();
      latch.countDown();
      return null;
    }).when(executor).execute(any());
    MergeQueries mergeQueries = mergeQueries(false, true, true);

    // Act
    mergeQueries.mergeFlush(INTERMEDIATE_TABLE);

    // Assert
    latch.await();
    verify(bigQuery, times(3)).query(any());
  }

  @Test(expected = BigQueryConnectException.class)
  public void testBigQueryRetryExceeded() throws InterruptedException {
    // Arrange
    mergeBatches.addToBatch(TEST_SINK_RECORD, INTERMEDIATE_TABLE, new HashMap<>());

    BigQueryError jobInternalError =
        new BigQueryError("invalidQuery", null,
            "Could not serialize access to table my_table due to concurrent update");
    when(bigQuery.query(any()))
        .thenThrow(new BigQueryException(400, "mock invalid query", jobInternalError));
    when(mergeBatches.destinationTableFor(INTERMEDIATE_TABLE)).thenReturn(DESTINATION_TABLE);
    when(mergeBatches.incrementBatch(INTERMEDIATE_TABLE)).thenReturn(0);
    when(mergeBatches.prepareToFlush(INTERMEDIATE_TABLE, 0)).thenReturn(true);
    CountDownLatch latch = new CountDownLatch(1);
    doAnswer(invocation -> {
      Runnable runnable = invocation.getArgument(0);
      runnable.run();
      latch.countDown();
      return null;
    }).when(executor).execute(any());
    MergeQueries mergeQueries = mergeQueries(false, true, true);

    // Act
    mergeQueries.mergeFlush(INTERMEDIATE_TABLE);

    //Assert
    latch.await();
  }

  private String table(TableId table) {
    return String.format("`%s`.`%s`", table.getDataset(), table.getTable());
  }
}
