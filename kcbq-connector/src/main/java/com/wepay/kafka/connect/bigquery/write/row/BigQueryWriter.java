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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * A class for writing lists of rows to a BigQuery table.
 */
public abstract class BigQueryWriter {

  private static final int FORBIDDEN = 403;
  private static final int INTERNAL_SERVICE_ERROR = 500;
  private static final int BAD_GATEWAY = 502;
  private static final int SERVICE_UNAVAILABLE = 503;
  private static final String QUOTA_EXCEEDED_REASON = "quotaExceeded";
  private static final String RATE_LIMIT_EXCEEDED_REASON = "rateLimitExceeded";

  private static final int WAIT_MAX_JITTER = 1000;

  private static final Logger logger = LoggerFactory.getLogger(BigQueryWriter.class);

  private static final Random random = new Random();

  private int retries;
  private long retryWaitMs;

  /**
   * @param retries the number of times to retry a request if BQ returns an internal service error
   *                or a service unavailable error.
   * @param retryWaitMs the amount of time to wait in between reattempting a request if BQ returns
   *                    an internal service error or a service unavailable error.
   */
  public BigQueryWriter(int retries, long retryWaitMs) {
    this.retries = retries;
    this.retryWaitMs = retryWaitMs;
  }

  /**
   * Handle the actual transmission of the write request to BigQuery, including any exceptions or
   * errors that happen as a result.
   * @param tableId The PartitionedTableId.
   * @param rows The rows to write.
   * @param topic The Kafka topic that the row data came from.
   * @return map from failed row id to the BigQueryError.
   */
  protected abstract Map<Long, List<BigQueryError>> performWriteRequest(PartitionedTableId tableId,
                                                                        List<InsertAllRequest.RowToInsert> rows,
                                                                        String topic)
      throws BigQueryException, BigQueryConnectException;

  /**
   * Create an InsertAllRequest.
   * @param tableId the table to insert into.
   * @param rows the rows to insert.
   * @return the InsertAllRequest.
   */
  protected InsertAllRequest createInsertAllRequest(PartitionedTableId tableId,
                                                    List<InsertAllRequest.RowToInsert> rows) {
    return InsertAllRequest.newBuilder(tableId.getFullTableId(), rows)
        .setIgnoreUnknownValues(false)
        .setSkipInvalidRows(false)
        .build();
  }

  /**
   * @param table The BigQuery table to write the rows to.
   * @param rows The rows to write.
   * @param topic The Kafka topic that the row data came from.
   * @throws InterruptedException if interrupted.
   */
  public void writeRows(PartitionedTableId table,
                        List<InsertAllRequest.RowToInsert> rows,
                        String topic)
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
        failedRowsMap = performWriteRequest(table, rows, topic);
        if (failedRowsMap.isEmpty()) {
          // table insertion completed with no reported errors
          return;
        } else if (isPartialFailure(rows, failedRowsMap)) {
            logger.info("{} rows succeeded, {} rows failed", rows.size() - failedRowsMap.size(), failedRowsMap.size());
            // update insert rows and retry in case of partial failure
            rows = getFailedRows(rows, failedRowsMap.keySet());
            mostRecentException = new BigQueryConnectException(failedRowsMap);
            retryCount++;
        } else {
            // throw an exception in case of complete failure
            throw new BigQueryConnectException(failedRowsMap);
        }
      } catch (BigQueryException err) {
        mostRecentException = err;
        if (err.getCode() == INTERNAL_SERVICE_ERROR
            || err.getCode() == SERVICE_UNAVAILABLE
            || err.getCode() == BAD_GATEWAY) {
          // backend error: https://cloud.google.com/bigquery/troubleshooting-errors
          /* for BAD_GATEWAY: https://cloud.google.com/storage/docs/json_api/v1/status-codes
             todo possibly this page is inaccurate for bigquery, but the message we are getting
             suggest it's an internal backend error and we should retry, so lets take that at face
             value. */
          logger.warn("BQ backend error: {}, attempting retry", err.getCode());
          retryCount++;
        } else if (err.getCode() == FORBIDDEN
                   && err.getError() != null
                   && QUOTA_EXCEEDED_REASON.equals(err.getReason())) {
          // quota exceeded error
          logger.warn("Quota exceeded for table {}, attempting retry", table);
          retryCount++;
        } else if (err.getCode() == FORBIDDEN
                   && err.getError() != null
                   && RATE_LIMIT_EXCEEDED_REASON.equals(err.getReason())) {
          // rate limit exceeded error
          logger.warn("Rate limit exceeded for table {}, attempting retry", table);
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
  private boolean isPartialFailure(List<InsertAllRequest.RowToInsert> rows, Map<Long, List<BigQueryError>> failedRowsMap) {
    return failedRowsMap.size() < rows.size();
  }

  /**
   * Filter out succeed rows, and return a list of failed rows.
   * @param rows The rows to write.
   * @param failRowsSet A set of failed row index.
   * @return A list of failed rows.
   */
  private List<InsertAllRequest.RowToInsert> getFailedRows(List<InsertAllRequest.RowToInsert> rows,
                                                           Set<Long> failRowsSet) {
    List<InsertAllRequest.RowToInsert> failRows = new ArrayList<>();
    for (int index = 0; index < rows.size(); index++) {
      if (failRowsSet.contains((long)index)) {
        logger.debug("[row with index {}]: failed", index);
        failRows.add(rows.get(index));
      }
    }
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
