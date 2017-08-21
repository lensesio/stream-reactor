package com.wepay.kafka.connect.bigquery.write.row;

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


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;

import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;

import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * A {@link BigQueryWriter} capable of updating BigQuery table schemas.
 */
public class AdaptiveBigQueryWriter extends BigQueryWriter {
  private static final Logger logger = LoggerFactory.getLogger(AdaptiveBigQueryWriter.class);

  // The maximum number of retries we will attempt to write rows after updating a BQ table schema.
  private static final int AFTER_UPDATE_RETY_LIMIT = 5;

  private final BigQuery bigQuery;
  private final SchemaManager schemaManager;

  /**
   * @param bigQuery Used to send write requests to BigQuery.
   * @param schemaManager Used to update BigQuery tables.
   * @param retry How many retries to make in the event of a 500/503 error.
   * @param retryWait How long to wait in between retries.
   */
  public AdaptiveBigQueryWriter(BigQuery bigQuery,
                                SchemaManager schemaManager,
                                int retry,
                                long retryWait) {
    super(retry, retryWait);
    this.bigQuery = bigQuery;
    this.schemaManager = schemaManager;
  }

  /**
   * Sends the request to BigQuery, then checks the response to see if any errors have occurred. If
   * any have, and all errors can be blamed upon invalid columns in the rows sent, attempts to
   * update the schema of the table in BigQuery and then performs the same write request.
   * @param tableId The PartitionedTableId.
   * @param rows The rows to write.
   * @param topic The Kafka topic that the row data came from.
   */
  @Override
  public void performWriteRequest(PartitionedTableId tableId,
                                  List<InsertAllRequest.RowToInsert> rows,
                                  String topic) {
    InsertAllRequest request = createInsertAllRequest(tableId, rows);
    InsertAllResponse writeResponse = bigQuery.insertAll(request);
    // Should only perform one schema update attempt; may have to continue insert attempts due to
    // BigQuery schema updates taking up to two minutes to take effect
    if (writeResponse.hasErrors()
        && onlyContainsInvalidSchemaErrors(writeResponse.getInsertErrors())) {
      try {
        schemaManager.updateSchema(tableId.getBaseTableId(), topic);
      } catch (BigQueryException exception) {
        throw new BigQueryConnectException("Failed to update table schema for: " + tableId.getBaseTableId(), exception);
      }
    }

    // Schema update might be delayed, so multiple insertion attempts may be necessary
    int attemptCount = 0;
    while (writeResponse.hasErrors()) {
      logger.trace("insertion failed");
      if (onlyContainsInvalidSchemaErrors(writeResponse.getInsertErrors())) {
        logger.debug("re-attempting insertion");
        writeResponse = bigQuery.insertAll(request);
      } else {
        throw new BigQueryConnectException(writeResponse.getInsertErrors());
      }
      attemptCount++;
      if (attemptCount >= AFTER_UPDATE_RETY_LIMIT) {
        throw new BigQueryConnectException("Failed to write rows after BQ schema update within "
                                           + AFTER_UPDATE_RETY_LIMIT + " attempts for: " + tableId.getBaseTableId());
      }
    }
    logger.debug("table insertion completed successfully");
  }

  /*
   * Currently, the only way to determine the cause of an insert all failure is by examining the map
   * object returned by the insertErrors() method of an insert all response. The only way to
   * determine the cause of each individual error is by manually examining each error's reason() and
   * message() strings, and guessing what they mean. Ultimately, the goal of this method is to
   * return whether or not an insertion failed due solely to a mismatch between the schemas of the
   * inserted rows and the schema of the actual BigQuery table.
   * This is why we can't have nice things, Google.
   */
  private boolean onlyContainsInvalidSchemaErrors(Map<Long, List<BigQueryError>> errors) {
    boolean invalidSchemaError = false;
    for (List<BigQueryError> errorList : errors.values()) {
      for (BigQueryError error : errorList) {
        if (error.getReason().equals("invalid") && error.getMessage().contains("no such field")) {
          invalidSchemaError = true;
        } else if (!error.getReason().equals("stopped")) {
          /* if some rows are in the old schema format, and others aren't, the old schema
           * formatted rows will show up as error: stopped. We still want to continue if this is
           * the case, because these errors don't represent a unique error if there are also
           * invalidSchemaErrors.
           */
          return false;
        }
      }
    }
    // if we only saw "stopped" errors, we want to return false. (otherwise, return true)
    return invalidSchemaError;
  }
}
