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

package com.wepay.kafka.connect.bigquery.config;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.TABLE_CREATE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.TOPICS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableExistenceValidatorTest {

  @Test
  public void testMissingTableWithAutoCreationDisabled() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getTopicsToDatasets())
        .thenReturn(ImmutableMap.of(
            "t1", "d1",
            "t2", "d2"
        ));
    when(config.getBoolean(eq(TABLE_CREATE_CONFIG))).thenReturn(false);
    when(config.getList(TOPICS_CONFIG)).thenReturn(Arrays.asList("t1", "t2"));

    BigQuery bigQuery = bigQuery(TableId.of("d1", "t1"));

    assertNotEquals(
        Optional.empty(),
        new TableExistenceValidator().doValidate(bigQuery, config)
    );
  }

  @Test
  public void testEmptyTopicsListWithAutoCreationDisabled() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getTopicsToDatasets())
        .thenReturn(ImmutableMap.of(
            "t1", "d1",
            "t2", "d2"
        ));
    when(config.getBoolean(eq(TABLE_CREATE_CONFIG))).thenReturn(false);
    when(config.getList(TOPICS_CONFIG)).thenReturn(Collections.emptyList());

    BigQuery bigQuery = bigQuery();

    assertEquals(
        Optional.empty(),
        new TableExistenceValidator().doValidate(bigQuery, config)
    );
  }

  @Test
  public void testMissingTableWithAutoCreationEnabled() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getBoolean(eq(TABLE_CREATE_CONFIG))).thenReturn(true);

    assertEquals(
        Optional.empty(),
        new TableExistenceValidator().doValidate(null, config)
    );
  }

  @Test
  public void testExactListOfMissingTables() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getTopicsToDatasets())
        .thenReturn(ImmutableMap.of(
            "t1", "d1",
            "t2", "d2",
            "t3", "d1",
            "t4", "d2",
            "t5", "d1"
        ));
    when(config.getList(TOPICS_CONFIG)).thenReturn(Arrays.asList("t1", "t2", "t3", "t4", "t5"));

    BigQuery bigQuery = bigQuery(
        TableId.of("d1", "t1"),
        TableId.of("d3", "t2"),
        TableId.of("d2", "t5")
    );
    Set<TableId> expectedMissingTables = new HashSet<>(Arrays.asList(
        TableId.of("d2", "t2"),
        TableId.of("d1", "t3"),
        TableId.of("d2", "t4"),
        TableId.of("d1", "t5")
    ));

    assertEquals(
        expectedMissingTables,
        new HashSet<>(new TableExistenceValidator().missingTables(bigQuery, config))
    );
  }

  @Test
  public void testExactEmptyListOfMissingTables() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getTopicsToDatasets())
        .thenReturn(ImmutableMap.of(
            "t1", "d1",
            "t2", "d2",
            "t3", "d1",
            "t4", "d2",
            "t5", "d1"
        ));
    when(config.getList(TOPICS_CONFIG)).thenReturn(Arrays.asList("t1", "t2", "t3", "t4", "t5"));

    BigQuery bigQuery = bigQuery(
        TableId.of("d1", "t1"),
        TableId.of("d2", "t2"),
        TableId.of("d1", "t3"),
        TableId.of("d2", "t4"),
        TableId.of("d1", "t5")
    );

    assertEquals(
        Collections.emptyList(),
        new TableExistenceValidator().missingTables(bigQuery, config)
    );
  }

  private static BigQuery bigQuery(TableId... existingTables) {
    BigQuery result = mock(BigQuery.class);
    Stream.of(existingTables).forEach(table -> {
      Table mockTable = mock(Table.class);
      when(result.getTable(eq(table))).thenReturn(mockTable);
    });
    return result;
  }
}
