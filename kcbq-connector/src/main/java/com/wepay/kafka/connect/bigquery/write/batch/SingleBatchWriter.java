package com.wepay.kafka.connect.bigquery.write.batch;

/*
 * Copyright 2016 WePay, Inc.
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


import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;

import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.Set;

/**
 * A batch writer that writes all given elements in a single batch request.
 */
public class SingleBatchWriter implements BatchWriter<InsertAllRequest.RowToInsert> {
  private BigQueryWriter writer;

  @Override
  public void init(BigQueryWriter writer) {
    this.writer = writer;
  }

  /**
   * @param elements The list of elements to write in a single batch.
   */
  @Override
  public void writeAll(PartitionedTableId table,
                       List<InsertAllRequest.RowToInsert> elements,
                       String topic,
                       Set<Schema> schemas)
      throws BigQueryConnectException, InterruptedException {
    try {
      writer.writeRows(table, elements, topic, schemas);
    } catch (BigQueryException err) {
      throw new BigQueryConnectException(
          String.format("Failed to write to BigQuery table %s", table),
          err);
    }

  }
}
