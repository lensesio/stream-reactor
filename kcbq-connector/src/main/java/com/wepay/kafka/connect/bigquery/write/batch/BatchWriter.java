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


import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;
import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.Set;

/**
 * Interface for splitting lists of elements into batches and writing those batches to BigQuery.
 * All classes that derive this class should be thread-safe.
 * @param <E> The type of element in the list that will be split.
 */
public interface BatchWriter<E> {

  /**
   * Initialize this BatchWriter. Necessary before calling
   * {@link #writeAll(TableId, List, String, Set)}
   *
   * @param writer the writer to use to write BigQuery requests.
   */
  void init(BigQueryWriter writer);

  /**
   * @param table The BigQuery table to write the rows to.
   * @param elements The list of elements to write to BigQuery.
   * @param topic The Kafka topic that the row data came from.
   * @param schemas The unique Schemas for the row data.
   * @throws BigQueryConnectException if we are unable to write to BigQuery
   * @throws InterruptedException if writing is interrupted
   */
  void writeAll(TableId table, List<E> elements, String topic, Set<Schema> schemas)
      throws BigQueryConnectException, InterruptedException;
}
