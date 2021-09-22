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

package com.wepay.kafka.connect.bigquery.integration;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.wepay.kafka.connect.bigquery.integration.utils.TableClearer;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryErrorResponses;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import static com.wepay.kafka.connect.bigquery.utils.TableNameUtils.table;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BigQueryErrorResponsesIT extends BaseConnectorIT {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryErrorResponsesIT.class);

  private BigQuery bigQuery;

  @Before
  public void setup() {
    bigQuery = newBigQuery();
  }

  @Test
  public void testWriteToNonExistentTable() {
    TableId table = TableId.of(dataset(), suffixedAndSanitizedTable("nonexistent table"));
    TableClearer.clearTables(bigQuery, dataset(), table.getTable());

    try {
      bigQuery.insertAll(InsertAllRequest.of(table, RowToInsert.of(Collections.singletonMap("f1", "v1"))));
      fail("Should have failed to write to nonexistent table");
    } catch (BigQueryException e) {
      logger.debug("Nonexistent table write error", e);
      assertTrue(BigQueryErrorResponses.isNonExistentTableError(e));
    }
  }

  @Test
  public void testWriteToTableWithoutSchema() {
    TableId table = TableId.of(dataset(), suffixedAndSanitizedTable("missing schema"));
    createOrAssertSchemaMatches(table, Schema.of());

    try {
      bigQuery.insertAll(InsertAllRequest.of(table, RowToInsert.of(Collections.singletonMap("f1", "v1"))));
      fail("Should have failed to write to table with no schema");
    } catch (BigQueryException e) {
      logger.debug("Table missing schema write error", e);
      assertTrue(BigQueryErrorResponses.isTableMissingSchemaError(e));
    }
  }

  @Test
  public void testWriteWithMissingRequiredFields() {
    TableId table = TableId.of(dataset(), suffixedAndSanitizedTable("too many fields"));
    Schema schema = Schema.of(
        Field.newBuilder("f1", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f3", StandardSQLTypeName.BOOL).setMode(Field.Mode.NULLABLE).build()
    );
    createOrAssertSchemaMatches(table, schema);

    InsertAllResponse response = bigQuery.insertAll(InsertAllRequest.of(table, RowToInsert.of(Collections.singletonMap("f2", 12L))));
    logger.debug("Write response errors for missing required field: {}", response.getInsertErrors());
    BigQueryError error = assertResponseHasSingleError(response);
    assertTrue(BigQueryErrorResponses.isMissingRequiredFieldError(error));
  }

  @Test
  public void testWriteWithUnrecognizedFields() {
    TableId table = TableId.of(dataset(), suffixedAndSanitizedTable("not enough fields"));
    Schema schema = Schema.of(
        Field.newBuilder("f1", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
    );
    createOrAssertSchemaMatches(table, schema);

    Map<String, Object> row = new HashMap<>();
    row.put("f1", "v1");
    row.put("f2", 12L);
    InsertAllResponse response = bigQuery.insertAll(InsertAllRequest.of(table, RowToInsert.of(row)));
    logger.debug("Write response errors for unrecognized field: {}", response.getInsertErrors());
    BigQueryError error = assertResponseHasSingleError(response);
    assertTrue(BigQueryErrorResponses.isUnrecognizedFieldError(error));
  }

  @Test
  public void testStoppedRowsDuringInvalidWrite() {
    TableId table = TableId.of(dataset(), suffixedAndSanitizedTable("not enough fields"));
    Schema schema = Schema.of(
        Field.newBuilder("f1", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
    );
    createOrAssertSchemaMatches(table, schema);

    Map<String, Object> row1 = new HashMap<>();
    row1.put("f1", "v1");
    row1.put("f2", 12L);
    Map<String, Object> row2 = Collections.singletonMap("f1", "v2");
    InsertAllResponse response = bigQuery.insertAll(InsertAllRequest.of(table, RowToInsert.of(row1), RowToInsert.of(row2)));
    logger.debug("Write response errors for unrecognized field and stopped row: {}", response.getInsertErrors());
    assertEquals(2, response.getInsertErrors().size());
    // As long as we have some kind of error on the first row it's fine; we want to be more precise in our assertions about the second row
    assertListHasSingleElement(response.getErrorsFor(0));
    BigQueryError secondRowError = assertListHasSingleElement(response.getErrorsFor(1));
    assertTrue(BigQueryErrorResponses.isStoppedError(secondRowError));
  }

  @Test
  public void testRequestPayloadTooLarge() {
    TableId table = TableId.of(dataset(), suffixedAndSanitizedTable("request payload too large"));
    Schema schema = Schema.of(
        Field.newBuilder("f1", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
    );
    createOrAssertSchemaMatches(table, schema);

    char[] chars = new char[10 * 1024 * 1024];
    Arrays.fill(chars, '*');
    String columnValue = new String(chars);
    try {
      bigQuery.insertAll(InsertAllRequest.of(table, RowToInsert.of(Collections.singletonMap("f1", columnValue))));
      fail("Should have failed to write to table with 11MB request");
    } catch (BigQueryException e) {
      logger.debug("Large request payload write error", e);
      assertTrue(BigQueryErrorResponses.isRequestTooLargeError(e));
    }
  }

  @Test
  public void testTooManyRows() {
    TableId table = TableId.of(dataset(), suffixedAndSanitizedTable("too many rows"));
    Schema schema = Schema.of(
        Field.newBuilder("f1", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build()
    );
    createOrAssertSchemaMatches(table, schema);

    Iterable<RowToInsert> rows = LongStream.range(0, 100_000)
        .mapToObj(i -> Collections.singletonMap("f1", i))
        .map(RowToInsert::of)
        .collect(Collectors.toList());
    try {
      bigQuery.insertAll(InsertAllRequest.of(table, rows));
      fail("Should have failed to write to table with 100,000 rows");
    } catch (BigQueryException e) {
      logger.debug("Too many rows write error", e);
      assertTrue(BigQueryErrorResponses.isTooManyRowsError(e));
    }
  }

  // Some tables can't be deleted, recreated, and written to without getting a temporary error from BigQuery,
  // so we just create them once if they don't exist and don't delete them at the end of the test.
  // If we detect a table left over (presumably from a prior test), we do a sanity check to make sure that it
  // has the expected schema.
  private void createOrAssertSchemaMatches(TableId tableId, Schema schema) {
    Table table = bigQuery.getTable(tableId);
    if (table == null) {
      bigQuery.create(TableInfo.newBuilder(tableId, StandardTableDefinition.of(schema)).build());
    } else {
      assertEquals(
          String.format("Testing %s should be created automatically by tests; please delete the table and re-run this test", table(tableId)),
          schema,
          table.getDefinition().getSchema()
      );
    }
  }

  private BigQueryError assertResponseHasSingleError(InsertAllResponse response) {
    assertEquals(1, response.getInsertErrors().size());
    Iterator<List<BigQueryError>> errorsIterator = response.getInsertErrors().values().iterator();
    assertTrue(errorsIterator.hasNext());
    return assertListHasSingleElement(errorsIterator.next());
  }

  private <T> T assertListHasSingleElement(List<T> list) {
    assertEquals(1, list.size());
    return list.get(0);
  }
}
