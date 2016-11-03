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
import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;

import org.apache.kafka.connect.data.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * A batch writer that attempts to find the largest non-erroring batch size and then evenly divides
 * all the given elements among the fewest possible batch requests, given the maximum allowable
 * batch size.
 */
public class DynamicBatchWriter implements BatchWriter<InsertAllRequest.RowToInsert> {
  private static final Logger logger = LoggerFactory.getLogger(DynamicBatchWriter.class);

  private static final int BAD_REQUEST_CODE = 400;

  private static final String INVALID_REASON = "invalid";

  // google only allows request batch sizes of up to 100000, so this is a hard maximum.
  private static final int MAXIMUM_BATCH_SIZE = 100000;
  private static final int INITIAL_BATCH_SIZE = 1000;

  // in non-seeking mode, the number of writeAlls that must complete successfully without error in
  // a row before we increase the batchSize.
  private static final int CONT_SUCCESS_COUNT_BUMP = 10;

  private BigQueryWriter writer;
  private int currentBatchSize;
  private boolean seeking;

  private int contSuccessCount;

  /**
   * create a new DynamicBatchWriter.
   */
  public DynamicBatchWriter() {
    this.currentBatchSize = INITIAL_BATCH_SIZE;
    this.seeking = true;
    this.contSuccessCount = 0;
  }

  // package private; for testing only
  DynamicBatchWriter(BigQueryWriter writer, int initialBatchSize, boolean seeking) {
    this.writer = writer;
    this.currentBatchSize = initialBatchSize;
    this.seeking = seeking;
    this.contSuccessCount = 0;
  }

  // package private; for testing only
  int getCurrentBatchSize() {
    return currentBatchSize;
  }

  // package private; for testing only
  boolean isSeeking() {
    return seeking;
  }

  @Override
  public void init(BigQueryWriter writer) {
    this.writer = writer;
  }

  @Override
  public synchronized void writeAll(TableId table,
                                    List<InsertAllRequest.RowToInsert> elements,
                                    String topic,
                                    Set<Schema> schemas) throws BigQueryConnectException,
                                                                InterruptedException {
    if (seeking) {
      seekingWriteAll(table, elements, topic, schemas);
    } else {
      establishedWriteAll(table, elements, topic, schemas);
    }
  }

  /**
   * A writeAll intended to quickly find the largest viable batchSize.
   *
   * <p>We start with the currentBatchSize and adjust up or down as necessary. Seek mode ends when
   * one of the following occurs:
   *   1. we get an error after at least 1 success. This is slightly inefficient if our
   *      currentBatchSize is too big, but we will only make 1 extra request in that case.
   *      Successes do not carry over from previous seekingWriteAll requests.
   *   2. we write all the given elements in one batch request without error.
   *   3. we write MAXIMUM_BATCH_SIZE elements in one batch request without error. (unlikely)
   */
  private void seekingWriteAll(TableId table,
                               List<InsertAllRequest.RowToInsert> elements,
                               String topic,
                               Set<Schema> schemas) throws BigQueryConnectException,
                                                           InterruptedException {
    logger.debug("Seeking best batch size...");
    int currentIndex = 0;
    int successfulCallCount = 0;
    while (currentIndex < elements.size()) {
      int endIndex = Math.min(currentIndex + currentBatchSize, elements.size());
      List<InsertAllRequest.RowToInsert> currentBatchElements =
          elements.subList(currentIndex, endIndex);
      try {
        writer.writeRows(table, currentBatchElements, topic, schemas);
        // success
        successfulCallCount++;
        currentIndex = endIndex;
        // if our batch size is the maximum batch size we are done finding a batch size.
        if (currentBatchSize == MAXIMUM_BATCH_SIZE) {
          seeking = false; // case 3
          logger.debug("Best batch size found (max): {}", currentBatchSize);
          establishedWriteAll(table,
                              elements.subList(currentIndex, elements.size()),
                              topic,
                              schemas);
          return;
        }
        // increase the batch size if there is more to test.
        if (currentIndex < elements.size()) {
          increaseBatchSize(table);
        }
      } catch (BigQueryException exception) {
        // failure
        if (isBatchSizeError(exception)) {
          decreaseBatchSize(table, exception);
          // if we've had at least 1 successful call we'll assume this is a good batch size.
          if (successfulCallCount > 0) {
            seeking = false; // case 1
            logger.debug("Best batch size found (error if higher): {}", currentBatchSize);
            establishedWriteAll(table,
                                elements.subList(currentIndex, elements.size()),
                                topic,
                                schemas);
            return;
          }
        } else {
          throw new BigQueryConnectException(
              String.format("Failed to write to BigQuery table %s", table),
              exception);
        }
      }
    }
    // if we finished all this with only one successful call...
    if (successfulCallCount ==  1) {
      // then we are done finding a batch size.
      seeking = false; // case 2
      logger.debug("Best batch size found (all elements): {}", currentBatchSize);
    }
  }

  /**
   * writeAll intended for a generally stable currentBatchSize.
   *
   * <p>Write all the given elements to BigQuery in the smallest possible number of batches, as
   * evenly as possible.
   *
   * <p>If we encounter a batch size related error, we will immediately lower the batch size and
   * try again.
   *
   * <p>Every {@link #CONT_SUCCESS_COUNT_BUMP} calls, if there have been no errors, we will bump up
   * the batch size.
   */
  private void establishedWriteAll(TableId table,
                                   List<InsertAllRequest.RowToInsert> elements,
                                   String topic,
                                   Set<Schema> schemas) throws BigQueryConnectException,
                                                               InterruptedException {
    int currentIndex = 0;
    while (currentIndex < elements.size()) {
      try {
        // handle case where no splitting is necessary:
        if (elements.size() <= currentBatchSize) {
          writer.writeRows(table, elements, topic, schemas);
          currentIndex = elements.size();
          // return; don't count this as a contSuccessCount because we don't want to increase
          // the batch size forever if we aren't going to be using it.
          return;
        }

        int elementsLeft = elements.size() - currentIndex;
        int numBatches = (int)Math.ceil(elementsLeft / (currentBatchSize * 1.0));
        int minBatchSize = elementsLeft / numBatches;
        int spareRows = elementsLeft % numBatches;

        for (int batchCount = 0; batchCount < numBatches; batchCount++) {
          // the first spareRows batches have an extra row in them.
          int batchSize = batchCount < spareRows ? minBatchSize + 1 : minBatchSize;
          int endIndex = Math.min(currentIndex + batchSize, elements.size());
          writer.writeRows(table, elements.subList(currentIndex, endIndex), topic, schemas);
          currentIndex = endIndex;
        }
      } catch (BigQueryException exception) {
        if (isBatchSizeError(exception)) {
          // immediately decrease batch size and try again with remaining elements.
          logger.debug("Batch size error during establishedWriteAll, reducing batch size.");
          decreaseBatchSize(table, exception);
          contSuccessCount = 0;
        } else {
          throw new BigQueryConnectException(
              String.format("Failed to write to BigQuery table %s", table),
              exception);
        }
      }
    }

    contSuccessCount++;
    if (contSuccessCount >= CONT_SUCCESS_COUNT_BUMP) {
      logger.debug("{} successful establishedWriteAlls in a row, increasing batchSize to {}",
                   contSuccessCount, currentBatchSize);
      contSuccessCount = 0;
      increaseBatchSize(table);
    }
  }

  /**
   * @param exception the {@link BigQueryException} to check.
   * @return true if this error is an error that can be fixed by retrying with a smaller batch
   *         size, or false otherwise.
   */
  private boolean isBatchSizeError(BigQueryException exception) {
    if (exception.code() == BAD_REQUEST_CODE
        && exception.error() == null
        && exception.reason() == null) {
      // 400 with no error or reason represents a request that is more than 10MB. This is not
      // documented but is referenced slightly under "Error codes" here:
      // https://cloud.google.com/bigquery/quota-policy
      // (by decreasing the batch size we can eventually expect to end up with a request under
      // 10MB)
      return true;
    } else if (exception.code() == BAD_REQUEST_CODE && INVALID_REASON.equals(exception.reason())) {
      // this is the error that the documentation claims google will return if a request exceeds
      // 10MB. if this actually ever happens...
      // todo distinguish this from other invalids (like invalid table schema).
      return true;
    }
    return false;
  }

  private void increaseBatchSize(TableId tableId) {
    currentBatchSize = Math.min(currentBatchSize * 2, MAXIMUM_BATCH_SIZE);
    logger.info("Increased batch size to {} for {}", currentBatchSize, tableId.toString());
  }

  private void decreaseBatchSize(TableId tableId, Exception reason) {
    logger.debug("Decreasing batch size due to error: {}", reason.getMessage());
    if (currentBatchSize <= 1) {
      // kafka source must have a huge row in it; we can't get past it, just error.
      throw new BigQueryConnectException("Attempted to decrease batchSize below 1", reason);
    }
    currentBatchSize = (int)Math.ceil(currentBatchSize / 2.0);
    logger.info("Decreased batch size to {} for {}", currentBatchSize, tableId.toString());
  }
}
