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

package com.wepay.kafka.connect.bigquery.exception;

import com.google.cloud.bigquery.BigQueryError;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.List;
import java.util.Map;

/**
 * Class for exceptions that occur while interacting with BigQuery, such as login failures, schema
 * update failures, and table insertion failures.
 */
public class BigQueryConnectException extends ConnectException {
  public BigQueryConnectException(String msg) {
    super(msg);
  }

  public BigQueryConnectException(String msg, Throwable thr) {
    super(msg, thr);
  }

  public BigQueryConnectException(Throwable thr) {
    super(thr);
  }

  public BigQueryConnectException(String tableInfo, Map<Long, List<BigQueryError>> errors) {
    super(formatInsertAllErrors(tableInfo, errors));
  }

  private static String formatInsertAllErrors(String tableInfo, Map<Long, List<BigQueryError>> errorsMap) {
    StringBuilder messageBuilder = new StringBuilder();
    messageBuilder.append(String.format("table: %s insertion failed for the following rows:", tableInfo));
    for (Map.Entry<Long, List<BigQueryError>> errorsEntry : errorsMap.entrySet()) {
      for (BigQueryError error : errorsEntry.getValue()) {
        messageBuilder.append(String.format(
            "%n\t[row index %d] (location %s, reason: %s): %s",
            errorsEntry.getKey(),
            error.getLocation(),
            error.getReason(),
            error.getMessage()
        ));
      }
    }
    return messageBuilder.toString();
  }

  @Override
  public String toString() {
    return getCause() != null ?
        super.toString() + "\nCaused by: " + getCause().getLocalizedMessage() : super.toString();
  }
}
