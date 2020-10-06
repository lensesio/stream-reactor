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

package com.wepay.kafka.connect.bigquery.utils;

import com.google.cloud.bigquery.TableId;

import java.time.Clock;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * A TableId with separate base table name and partition information.
 */
public class PartitionedTableId {

  private static final String PARTITION_DELIMITER = "$";
  private static final Clock UTC_CLOCK = Clock.systemUTC();
  private static final Long MILLIS_IN_DAY = 86400000L;

  private final String project;
  private final String dataset;
  private final String table;
  private final String partition;

  private final String fullTableName;

  private final TableId baseTableId;
  private final TableId fullTableId;

  /**
   * Create a new {@link PartitionedTableId}
   *
   * @param project The project name, if specified.
   * @param dataset The dataset name.
   * @param table The table name.
   * @param partition The partition of the table, if any.
   */
  private PartitionedTableId(String project, String dataset, String table, String partition) {
    this.project = project;
    this.dataset = dataset;
    this.table = table;
    this.partition = partition;

    fullTableName = createFullTableName(table, partition);

    baseTableId = createTableId(project, dataset, table);
    fullTableId = createTableId(project, dataset, fullTableName);
  }

  /**
   * Create and return a full table name from a base table name and a partition.
   * @param table the base table name.
   * @param partition the partition, if any.
   * @return full table name, including partition; Just the given table if no partition.
   */
  private static String createFullTableName(String table, String partition) {
    if (partition == null) {
      return table;
    } else {
      return table + PARTITION_DELIMITER + partition;
    }
  }

  /**
   * Convenience method for creating new TableIds.
   *
   * <p>{@link TableId#of(String, String, String)} will error if you pass in a null project, so this
   * just checks if the project is null and calls the correct method.
   *
   * @param project the project name, or null
   * @param dataset the dataset name
   * @param table the table name
   * @return a new TableId with the given project, dataset, and table.
   */
  private static TableId createTableId(String project, String dataset, String table) {
    if (project == null) {
      return TableId.of(dataset, table);
    } else {
      return TableId.of(project, dataset, table);
    }
  }

  public String getProject() {
    return project;
  }

  public String getDataset() {
    return dataset;
  }

  public String getBaseTableName() {
    return table;
  }

  public String getPartition() {
    return partition;
  }

  public String getFullTableName() {
    return fullTableName;
  }

  /**
   * Return the base table id, NOT containing any partition information in the table name.
   * This is useful if you only need general information about a table, like if you need to store
   * a table's schema information.
   *
   * @return the base table id.
   */
  public TableId getBaseTableId() {
    return baseTableId;
  }

  /**
   * Return the full table id, containing any partitioning information in the table name.
   * This is useful when you are attempting to actually write to this table.
   *
   * @return the full table id.
   */
  public TableId getFullTableId() {
    return fullTableId;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PartitionedTableId) {
      PartitionedTableId that = (PartitionedTableId) obj;
      return fullTableId.equals(that.fullTableId);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return fullTableId.hashCode();
  }

  @Override
  public String toString() {
    return fullTableId.toString();
  }

  public static class Builder {

    private String project;
    private final String dataset;
    private final String baseTable;
    private String partition;

    /**
     * Initialize a new {@link PartitionedTableId} Builder with a dataset and base table name.
     *
     * @param dataset the dataset name of the eventual PartitionedTableId.
     * @param baseTable the base table name of the eventual PartitionedTableId.
     */
    public Builder(String dataset, String baseTable) {
      this(null, dataset, baseTable, null);
    }

    /**
     * Initialize a new {@link PartitionedTableId} Builder with an existing base {@link TableId}.
     *
     * @param baseTableId existing TableId with a base table name (no partition information).
     */
    public Builder(TableId baseTableId) {
      this(baseTableId.getProject(), baseTableId.getDataset(), baseTableId.getTable(), null);
    }

    private Builder(String project, String dataset, String baseTable, String partition) {
      this.project = project;
      this.dataset = dataset;
      this.baseTable = baseTable;
      this.partition = partition;
    }

    public Builder setProject(String project) {
      this.project = project;
      return this;
    }

    public Builder setPartition(String partition) {
      this.partition = partition;
      return this;
    }

    public Builder setDayPartition(long utcTime) {
      return setDayPartition(LocalDate.ofEpochDay(utcTime / MILLIS_IN_DAY));
    }

    public Builder setDayPartition(LocalDate localDate) {
      return setPartition(dateToDayPartition(localDate));
    }

    public Builder setDayPartitionForNow() {
      return setDayPartition(LocalDate.now(UTC_CLOCK));
    }

    /**
     * @param localDate the localDate of the partition.
     * @return The String representation of the partition.
     */
    private static String dateToDayPartition(LocalDate localDate) {
      return localDate.format(DateTimeFormatter.BASIC_ISO_DATE);
    }

    /**
     * Build the {@link PartitionedTableId}.
     *
     * @return a {@link PartitionedTableId}.
     */
    public PartitionedTableId build() {
      return new PartitionedTableId(project, dataset, baseTable, partition);
    }
  }
}
