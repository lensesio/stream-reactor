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

import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;

/**
 * Handles the logic for classifying BigQuery error responses and determining things like whether they come from an
 * invalid schema error, a backend error, etc. This can be used to determine whether a table needs to be created before
 * retrying an insert or if a temporary server-side error requires us to retry a request, for example.
 */
public class BigQueryErrorResponses {

  private static final int BAD_REQUEST_CODE = 400;
  private static final int FORBIDDEN_CODE = 403;
  private static final int NOT_FOUND_CODE = 404;
  private static final int INTERNAL_SERVICE_ERROR_CODE = 500;
  private static final int BAD_GATEWAY_CODE = 502;
  private static final int SERVICE_UNAVAILABLE_CODE = 503;

  private static final String BAD_REQUEST_REASON = "badRequest";
  private static final String INVALID_REASON = "invalid";
  private static final String NOT_FOUND_REASON = "notFound";
  private static final String QUOTA_EXCEEDED_REASON = "quotaExceeded";
  private static final String RATE_LIMIT_EXCEEDED_REASON = "rateLimitExceeded";
  private static final String STOPPED_REASON = "stopped";


  public static boolean isNonExistentTableError(BigQueryException exception) {
    String message = message(exception.getError());
    // If a table does not exist, it will raise a BigQueryException that the input is notFound
    // Referring to Google Cloud Error Codes Doc: https://cloud.google.com/bigquery/docs/error-messages?hl=en
    return NOT_FOUND_CODE == exception.getCode()
        && NOT_FOUND_REASON.equals(exception.getReason())
        && (message.startsWith("Not found: Table ") || message.contains("Table is deleted: "));
  }

  public static boolean isTableMissingSchemaError(BigQueryException exception) {
    // If a table is missing a schema, it will raise a BigQueryException that the input is invalid
    // For more information about BigQueryExceptions, see: https://cloud.google.com/bigquery/troubleshooting-errors
    return BAD_REQUEST_CODE == exception.getCode()
        && INVALID_REASON.equals(exception.getReason())
        && message(exception.getError()).equals("The destination table has no schema.");
  }

  public static boolean isBackendError(BigQueryException exception) {
    // backend error: https://cloud.google.com/bigquery/troubleshooting-errors
    // for BAD_GATEWAY: https://cloud.google.com/storage/docs/json_api/v1/status-codes
    // TODO: possibly this page is inaccurate for bigquery, but the message we are getting
    //       suggest it's an internal backend error and we should retry, so lets take that at face
    //       value
    return INTERNAL_SERVICE_ERROR_CODE == exception.getCode()
        || BAD_GATEWAY_CODE  == exception.getCode()
        || SERVICE_UNAVAILABLE_CODE  == exception.getCode();
  }

  public static boolean isUnspecifiedBadRequestError(BigQueryException exception) {
    return BAD_REQUEST_CODE == exception.getCode()
        && exception.getError() == null
        && exception.getReason() == null;
  }

  public static boolean isQuotaExceededError(BigQueryException exception) {
    return FORBIDDEN_CODE == exception.getCode()
        // TODO: May be able to use exception.getReason() instead of (indirectly) exception.getError().getReason()
        //       Haven't been able to test yet though, so keeping as-is to avoid breaking anything
        && QUOTA_EXCEEDED_REASON.equals(reason(exception.getError()));
  }

  public static boolean isRateLimitExceededError(BigQueryException exception) {
    return FORBIDDEN_CODE == exception.getCode()
        // TODO: May be able to use exception.getReason() instead of (indirectly) exception.getError().getReason()
        //       Haven't been able to test yet though, so keeping as-is to avoid breaking anything
        && RATE_LIMIT_EXCEEDED_REASON.equals(reason(exception.getError()));
  }

  public static boolean isRequestTooLargeError(BigQueryException exception) {
    return BAD_REQUEST_CODE == exception.getCode()
        && BAD_REQUEST_REASON.equals(exception.getReason())
        && message(exception.getError()).startsWith("Request payload size exceeds the limit: ");
  }

  public static boolean isTooManyRowsError(BigQueryException exception) {
    return BAD_REQUEST_CODE == exception.getCode()
        && INVALID_REASON.equalsIgnoreCase(exception.getReason())
        && message(exception.getError()).startsWith("too many rows present in the request");
  }

  public static boolean isIOError(BigQueryException error) {
    return BigQueryException.UNKNOWN_CODE == error.getCode()
        && error.getCause() instanceof IOException;
  }

  public static boolean isUnrecognizedFieldError(BigQueryError error) {
    return INVALID_REASON.equals(reason(error))
        && message(error).startsWith("no such field: ");
  }

  public static boolean isMissingRequiredFieldError(BigQueryError error) {
    return INVALID_REASON.equals(reason(error))
        && message(error).startsWith("Missing required field");
  }

  public static boolean isStoppedError(BigQueryError error) {
    return STOPPED_REASON.equals(reason(error))
        && message(error).equals("");
  }

  private static String reason(BigQueryError error) {
    return extractFromError(error, BigQueryError::getReason);
  }

  private static String message(BigQueryError error) {
    return extractFromError(error, BigQueryError::getMessage);
  }

  private static String extractFromError(BigQueryError error, Function<BigQueryError, String> extraction) {
    return Optional.ofNullable(error)
        .map(extraction)
        .orElse("");
  }
}
