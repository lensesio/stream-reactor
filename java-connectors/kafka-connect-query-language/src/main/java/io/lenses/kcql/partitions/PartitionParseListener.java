/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.kcql.partitions;

import io.lenses.kcql.Kcql;
import io.lenses.kcql.antlr4.ConnectorParser;
import io.lenses.kcql.antlr4.ConnectorParserBaseListener;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PartitionParseListener extends ConnectorParserBaseListener {

  private final Kcql kcql;
  private final List<String> partitionBy = new ArrayList<>();

  public PartitionParseListener(Kcql kcql) {
    this.kcql = kcql;
  }

  @Override
  public void exitPartition_name(ConnectorParser.Partition_nameContext ctx) {
    addPartitionByField(ctx.getText());
  }

  @Override
  public void exitPartitionby(ConnectorParser.PartitionbyContext ctx) {
    if (ctx.NOPARTITION() == null) {
      kcql.setPartitions(new Partitions(partitionBy.stream().collect(Collectors.toUnmodifiableList())));
    } else {
      kcql.setPartitions(new NoPartitions());
    }
  }

  void addPartitionByField(final String field) {
    if (field == null || field.trim().isEmpty()) {
      throw new IllegalArgumentException("Invalid partition by field");
    }
    for (final String f : partitionBy) {
      if (f.compareToIgnoreCase(field.trim()) == 0) {
        throw new IllegalArgumentException(String.format("The field %s appears twice", field));
      }
    }
    partitionBy.add(field.trim());
  }

}
