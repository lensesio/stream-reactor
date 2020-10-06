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

package com.wepay.kafka.connect.bigquery.write.batch;

import com.google.cloud.bigquery.TableId;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Interface for building a {@link TableWriter} or TableWriterGCS.
 */
public interface TableWriterBuilder {

  /**
   * Add a record to the builder.
   * @param sinkRecord the row to add.
   * @param table the table the row will be written to.
   */
  void addRow(SinkRecord sinkRecord, TableId table);

  /**
   * Create a {@link TableWriter} from this builder.
   * @return a TableWriter containing the given writer, table, topic, and all added rows.
   */
  Runnable build();
}
