/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.kcql;

import io.lenses.kcql.partitions.NoPartitions;
import io.lenses.kcql.partitions.Partitions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class KcqlPartitionByTest {

  private final String topic = "TOPIC_A";
  private final String table = "TABLE_A";

  @Test
  void handlePartitionByWhenAllFieldsAreIncluded() {

    String syntax =
        String.format("UPSERT INTO %s SELECT * FROM %s IGNORE col1, 1col2 PARTITIONBY col1,col2  ", table, topic);
    Kcql kcql = Kcql.parse(syntax);

    assertThat(kcql.getPartitionBy()).toIterable().containsExactlyInAnyOrder("col1", "col2");

  }

  @Test
  void handlePartitionByFromHeader() {

    String syntax =
        String.format("UPSERT INTO %s SELECT * FROM %s IGNORE col1, 1col2 PARTITIONBY _header.col1,_header.col2  ",
            table, topic);
    Kcql kcql = Kcql.parse(syntax);

    assertThat(kcql.getPartitionBy()).toIterable().containsExactlyInAnyOrder("_header.col1", "_header.col2");
  }

  @Test
  void partitionByShouldAllowQuotingGroupsOfFields() {

    String syntax =
        String.format(
            "UPSERT INTO %s SELECT * FROM %s IGNORE col1, 1col2 PARTITIONBY _header.cost.centre.id,_header.`cost.centre.id`  ",
            topic, table);
    Kcql kcql = Kcql.parse(syntax);

    assertThat(kcql.getPartitionBy()).toIterable().hasSize(2).containsExactlyInAnyOrder("_header.cost.centre.id",
        "_header.`cost.centre.id`");
  }

  @Test
  void handlePartitionByWhenSpecificFieldsAreIncluded() {

    String syntax =
        String.format("UPSERT INTO %s SELECT col1, col2, col3 FROM %s IGNORE col1, 1col2 PARTITIONBY col1,col2  ",
            table, topic);
    Kcql kcql = Kcql.parse(syntax);

    assertThat(kcql.getPartitionBy()).toIterable().containsExactlyInAnyOrder("col1", "col2");
  }

  @Test
  void handlePartitionByWhenSpecificFieldsAreIncludedAndAliasingIsPresent() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format(
            "UPSERT INTO %s SELECT col1, col2 as colABC, col3 FROM %s IGNORE col1, 1col2 PARTITIONBY col1,colABC ",
            table, topic);
    Kcql kcql = Kcql.parse(syntax);

    assertThat(kcql.getPartitionBy()).toIterable().containsExactlyInAnyOrder("col1", "colABC");
  }

  @Test
  void allowNoPartition() {

    String syntax =
        String.format("UPSERT INTO %s SELECT * FROM %s NOPARTITION ", table, topic);
    Kcql kcql = Kcql.parse(syntax);

    assertEquals(new NoPartitions(), kcql.getPartitions());

  }

  @Test
  void allowEmptyPartition() {

    String syntax =
        String.format("UPSERT INTO %s SELECT * FROM %s ", table, topic);
    Kcql kcql = Kcql.parse(syntax);

    assertThat(kcql.getPartitions())
        .isOfAnyClassIn(Partitions.class)
        .hasFieldOrPropertyWithValue("partitionBy", List.of());

  }

  @Test
  void treatUndefinedPartitionAsEmptyFields() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("UPSERT INTO %s SELECT * FROM %s IGNORE col1, 1col2 PARTITIONBY col1,col2  ", table, topic);
    Kcql kcql = Kcql.parse(syntax);

    assertThat(kcql.getPartitionBy()).toIterable().containsExactlyInAnyOrder("col1", "col2");

  }

}
