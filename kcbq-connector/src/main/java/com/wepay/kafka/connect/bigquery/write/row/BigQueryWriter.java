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

package com.wepay.kafka.connect.bigquery.write.row;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A class for writing lists of rows to a BigQuery table.
 */
public abstract class BigQueryWriter {

  private static final int WAIT_MAX_JITTER = 1000;

  private static final Logger logger = LoggerFactory.getLogger(BigQueryWriter.class);

  private final int retries;
  private final long retryWaitMs;
  private final Random random;

  /**
   * @param retries the number of times to retry a request if BQ returns an internal service error
   *                or a service unavailable error.
   * @param retryWaitMs the amount of time to wait in between reattempting a request if BQ returns
   *                    an internal service error or a service unavailable error.
   */
  public BigQueryWriter(int retries, long retryWaitMs) {
    this.retries = retries;
    this.retryWaitMs = retryWaitMs;

    this.random = new Random();
  }

  /**
   * Handle the actual transmission of the write request to BigQuery, including any exceptions or
   * errors that happen as a result.
   * @param tableId The PartitionedTableId.
   * @param rows The rows to write.
   * @return map from failed row id to the BigQueryError.
   */
  protected abstract Map<Long, List<BigQueryError>> performWriteRequest(
          PartitionedTableId tableId,
          SortedMap<SinkRecord, InsertAllRequest.RowToInsert> rows)
      throws BigQueryException, BigQueryConnectException;

  /**
   * Create an InsertAllRequest.
   * @param tableId the table to insert into.
   * @param rows the rows to insert.
   * @return the InsertAllRequest.
   */
  protected InsertAllRequest createInsertAllRequest(PartitionedTableId tableId,
                                                    Collection<InsertAllRequest.RowToInsert> rows) {
    return InsertAllRequest.newBuilder(tableId.getFullTableId(), rows)
        .setIgnoreUnknownValues(false)
        .setSkipInvalidRows(false)
        .build();
  }

  /**
   * @param table The BigQuery table to write the rows to.
   * @param rows The rows to write.
   * @throws InterruptedException if interrupted.
   */
  public void writeRows(PartitionedTableId table,
                        SortedMap<SinkRecord, InsertAllRequest.RowToInsert> rows)
      throws BigQueryConnectException, BigQueryException, InterruptedException {
    logger.debug("writing {} row{} to table {}", rows.size(), rows.size() != 1 ? "s" : "", table);

    Exception mostRecentException = null;
    Map<Long, List<BigQueryError>> failedRowsMap = null;

    int retryCount = 0;
    do {
      if (retryCount > 0) {
        waitRandomTime();
      }
      try {
        failedRowsMap = performWriteRequest(table, rows);
        if (failedRowsMap.isEmpty()) {
          // table insertion completed with no reported errors
          return;
        } else if (isPartialFailure(rows, failedRowsMap)) {
          logger.info("{} rows succeeded, {} rows failed",
              rows.size() - failedRowsMap.size(), failedRowsMap.size());
          // update insert rows and retry in case of partial failure
          rows = getFailedRows(rows, failedRowsMap.keySet(), table);
          mostRecentException = new BigQueryConnectException(failedRowsMap);
          retryCount++;
        } else {
          // throw an exception in case of complete failure
          throw new BigQueryConnectException(failedRowsMap);
        }
      } catch (BigQueryException err) {
        mostRecentException = err;
        if (BigQueryErrorResponses.isBackendError(err)) {
          logger.warn("BQ backend error: {}, attempting retry", err.getCode());
          retryCount++;
        } else if (BigQueryErrorResponses.isQuotaExceededError(err)) {
          logger.warn("Quota exceeded for table {}, attempting retry", table);
          retryCount++;
        } else if (BigQueryErrorResponses.isRateLimitExceededError(err)) {
          logger.warn("Rate limit exceeded for table {}, attempting retry", table);
          retryCount++;
        } else if (BigQueryErrorResponses.isIOError(err)){
          logger.warn("IO Exception: {}, attempting retry", err.getCause().getMessage());
          retryCount++;
        } else {
          throw err;
        }
      }
    } while (retryCount <= retries);
    throw new BigQueryConnectException(
        String.format("Exceeded configured %d attempts for write request", retries),
        mostRecentException);
  }

  /**
   * Decide whether the failure is a partial failure or complete failure
   * @param rows The rows to write.
   * @param failedRowsMap A map from failed row index to the BigQueryError.
   * @return isPartialFailure.
   */
  private boolean isPartialFailure(SortedMap<SinkRecord, InsertAllRequest.RowToInsert> rows,
                                   Map<Long, List<BigQueryError>> failedRowsMap) {
    return failedRowsMap.size() < rows.size();
  }

  /**
   * Filter out succeed rows, and return a list of failed rows.
   * @param rows The rows to write.
   * @param failRowsSet A set of failed row index.
   * @return A list of failed rows.
   */
  private SortedMap<SinkRecord, InsertAllRequest.RowToInsert> getFailedRows(SortedMap<SinkRecord, InsertAllRequest.RowToInsert> rows,
                                                           Set<Long> failRowsSet,
                                                           PartitionedTableId table) {
    SortedMap<SinkRecord, InsertAllRequest.RowToInsert> failRows = new TreeMap<>(rows.comparator());
    int index = 0;
    for (Map.Entry<SinkRecord, InsertAllRequest.RowToInsert> row: rows.entrySet()) {
      if (failRowsSet.contains((long)index)) {
        failRows.put(row.getKey(), row.getValue());
      }
      index++;
    }
    logger.debug("{} rows failed to be written to table {}.", rows.size(), table.getFullTableName());
    return failRows;
  }

  /**
   * Wait at least {@link #retryWaitMs}, with up to an additional 1 second of random jitter.
   * @throws InterruptedException if interrupted.
   */
  private void waitRandomTime() throws InterruptedException {
    // wait
    Thread.sleep(retryWaitMs + random.nextInt(WAIT_MAX_JITTER));
  }
}
