package com.wepay.kafka.connect.bigquery.write;

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
import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * A class for writing lists of rows to a BigQuery table.
 */
public abstract class BigQueryWriter {

  private static final int INTERNAL_SERVICE_ERROR = 500;
  private static final int SERVICE_UNAVAILABLE = 503;

  private static final Logger logger = LoggerFactory.getLogger(BigQueryWriter.class);

  private int retries;
  private long retryWaitMs;

  public BigQueryWriter(int retries, long retryWaitMs) {
    this.retries = retries;
    this.retryWaitMs = retryWaitMs;
  }

  /**
   * Handle the actual transmission of the write request to BigQuery, including any exceptions or
   * errors that happen as a result.
   * @param request The request to send to BigQuery.
   * @param topic The Kafka topic that the row data came from.
   * @param schemas The unique Schemas for the row data.
   */
  protected abstract void performWriteRequest(
      InsertAllRequest request,
      String topic,
      Set<Schema> schemas
  );

  /**
   * @param table The BigQuery table to write the rows to.
   * @param rows The rows to write.
   * @param topic The Kafka topic that the row data came from.
   * @param schemas The unique Schemas for the row data.
   */
  public void writeRows(
      TableId table,
      List<InsertAllRequest.RowToInsert> rows,
      String topic,
      Set<Schema> schemas) {
    logger.debug("writing {} row{} to table {}", rows.size(), rows.size() != 1 ? "s" : "", table);

    int retryCount = 0;
    do {
      if (retryCount > 0) {
        try {
          Thread.sleep(retryWaitMs);
        } catch (InterruptedException err) {
          throw new ConnectException("Interrupted while waiting for BigQuery retry", err);
        }
      }
      try {
        performWriteRequest(
            InsertAllRequest.builder(table, rows)
                .ignoreUnknownValues(false)
                .skipInvalidRows(false)
                .build(),
            topic,
            schemas
        );
        return;
      } catch (BigQueryException err) {
        if (err.code() == INTERNAL_SERVICE_ERROR || err.code() == SERVICE_UNAVAILABLE) {
          // backend error: https://cloud.google.com/bigquery/troubleshooting-errors
          retryCount++;
        } else {
          throw new BigQueryConnectException("Failed to write to BigQuery table " + table, err);
        }
      }
    } while (retryCount <= retries);
  }
}
