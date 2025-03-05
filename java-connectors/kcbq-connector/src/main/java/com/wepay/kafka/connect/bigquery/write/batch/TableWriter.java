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

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;

import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.ExpectedInterruptException;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryErrorResponses;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Simple Table Writer that attempts to write all the rows it is given at once.
 */
public class TableWriter implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(TableWriter.class);

  private final BigQueryWriter writer;
  private final PartitionedTableId table;
  private final SortedMap<SinkRecord, RowToInsert> rows;
  private final Consumer<Collection<RowToInsert>> onFinish;

  /**
   * @param writer the {@link BigQueryWriter} to use.
   * @param table the BigQuery table to write to.
   * @param rows the rows to write.
   * @param onFinish a callback to invoke after all rows have been written successfully, which is
   *                 called with all the rows written by the writer
   */
  public TableWriter(BigQueryWriter writer,
                     PartitionedTableId table,
                     SortedMap<SinkRecord, RowToInsert> rows,
                     Consumer<Collection<RowToInsert>> onFinish) {
    this.writer = writer;
    this.table = table;
    this.rows = rows;
    this.onFinish = onFinish;
  }

  @Override
  public void run() {
    int currentIndex = 0;
    int currentBatchSize = rows.size();
    int successCount = 0;
    int failureCount = 0;

    List<Map.Entry<SinkRecord, RowToInsert>> rowsList = new ArrayList<>(rows.entrySet());
    try {
      while (currentIndex < rows.size()) {
        List<Map.Entry<SinkRecord, RowToInsert>> currentBatchList =
                rowsList.subList(currentIndex, Math.min(currentIndex + currentBatchSize, rows.size()));
        try {
          SortedMap<SinkRecord, RowToInsert> currentBatch = new TreeMap<>(rows.comparator());
          for (Map.Entry<SinkRecord, RowToInsert> record: currentBatchList) {
            currentBatch.put(record.getKey(), record.getValue());
          }
          writer.writeRows(table, currentBatch);
          currentIndex += currentBatchSize;
          successCount++;
        } catch (BigQueryException err) {
          logger.warn(
              "Could not write batch of size {} to BigQuery. "
                  + "Error code: {}, underlying error (if present): {}",
              currentBatchList.size(), err.getCode(), err.getError(), err);
          if (isBatchSizeError(err)) {
            failureCount++;
            currentBatchSize = getNewBatchSize(currentBatchSize, err);
          } else {
            // Throw exception on write errors such as 403.
            throw new BigQueryConnectException("Failed to write to table", err);
          }
        }
      }
    } catch (InterruptedException err) {
      throw new ExpectedInterruptException("Thread interrupted while writing to BigQuery.");
    }

    // Common case is 1 successful call and 0 failed calls:
    // Write to info if uncommon case,
    // Write to debug if common case
    String logMessage = "Wrote {} rows over {} successful calls and {} failed calls.";
    if (successCount + failureCount > 1) {
      logger.info(logMessage, rows.size(), successCount, failureCount);
    } else {
      logger.debug(logMessage, rows.size(), successCount, failureCount);
    }

    onFinish.accept(rows.values());
  }

  private static int getNewBatchSize(int currentBatchSize, Throwable err) {
    if (currentBatchSize == 1) {
      logger.error("Attempted to reduce batch size below 1");
      throw new BigQueryConnectException(
          "Failed to write to BigQuery even after reducing batch size to 1 row at a time. "
              + "This can indicate an error in the connector's logic for classifying BigQuery errors, as non-retriable"
              + "errors may be being treated as retriable."
              + "If that appears to be the case, please report the issue to the project's maintainers and include the "
              + "complete stack trace for this error as it appears in the logs. "
              + "Alternatively, there may be a record that the connector has read from Kafka that is too large to "
              + "write to BigQuery using the streaming insert API, which cannot be addressed with a change to the "
              + "connector and will need to be handled externally by optionally writing the record to BigQuery using "
              + "another means and then reconfiguring the connector to skip the record. "
              + "Finally, streaming insert quotas for BigQuery may be causing insertion failures for the connector; "
              + "in that case, please ensure that quotas for maximum rows per second, maximum bytes per second, etc. "
              + "are being respected before restarting the connector. "
              + "The cause of this exception is the error encountered from BigQuery after the last attempt to write a "
              + "batch was made.",
          err
      );
    }
    // round batch size up so we don't end up with a dangling 1 row at the end.
    return (int) Math.ceil(currentBatchSize / 2.0);
  }

  /**
   * @param exception the {@link BigQueryException} to check.
   * @return true if this error is an error that can be fixed by retrying with a smaller batch
   *         size, or false otherwise.
   */
  private static boolean isBatchSizeError(BigQueryException exception) {
    /*
     * 400 with no error or reason represents a request that is more than 10MB. This is not
     * documented but is referenced slightly under "Error codes" here:
     * https://cloud.google.com/bigquery/quota-policy
     * (by decreasing the batch size we can eventually expect to end up with a request under 10MB)
     */
    return BigQueryErrorResponses.isUnspecifiedBadRequestError(exception)
        || BigQueryErrorResponses.isRequestTooLargeError(exception)
        || BigQueryErrorResponses.isTooManyRowsError(exception);
  }


  public static class Builder implements TableWriterBuilder {
    private final BigQueryWriter writer;
    private final PartitionedTableId table;

    private SortedMap<SinkRecord, RowToInsert> rows;
    private SinkRecordConverter recordConverter;
    private Consumer<Collection<RowToInsert>> onFinish;

    /**
     * @param writer the BigQueryWriter to use
     * @param table the BigQuery table to write to.
     * @param recordConverter the record converter used to convert records to rows
     */
    public Builder(BigQueryWriter writer, PartitionedTableId table, SinkRecordConverter recordConverter) {
      this.writer = writer;
      this.table = table;

      this.rows = new TreeMap<>(Comparator.comparing(SinkRecord::kafkaPartition)
              .thenComparing(SinkRecord::kafkaOffset));
      this.recordConverter = recordConverter;

      this.onFinish = null;
    }

    @Override
    public void addRow(SinkRecord record, TableId table) {
      rows.put(record, recordConverter.getRecordRow(record, table));
    }

    /**
     * Specify a callback to be invoked after all rows have been written. The callback will be
     * invoked with the full list of rows written by this table writer.
     * @param onFinish the callback to invoke; may not be null
     * @throws IllegalStateException if invoked more than once on a single builder instance
     */
    public void onFinish(Consumer<Collection<RowToInsert>> onFinish) {
      if (this.onFinish != null) {
        throw new IllegalStateException("Cannot overwrite existing finish callback");
      }
      this.onFinish = Objects.requireNonNull(onFinish, "Finish callback cannot be null");
    }

    @Override
    public TableWriter build() {
      return new TableWriter(writer, table, rows, onFinish != null ? onFinish : n -> { });
    }
  }
}
