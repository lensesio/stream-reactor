package com.wepay.kafka.connect.bigquery.exception;


import com.google.cloud.bigquery.storage.v1.RowError;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.List;
import java.util.Map;

/**
 * Exception Class for exceptions that occur while interacting with BigQuery Storage Write API, such as login failures, schema
 * update failures, and table insertion failures.
 */
public class BigQueryStorageWriteApiConnectException extends ConnectException {

    public BigQueryStorageWriteApiConnectException(String message) {
        super(message);
    }

    public BigQueryStorageWriteApiConnectException(String message, Throwable error) {
        super(message, error);
    }

    public BigQueryStorageWriteApiConnectException(String tableName, List<RowError> errors) {
        super(formatRowErrors(tableName, errors));
    }

    public BigQueryStorageWriteApiConnectException(String tableName, Map<Integer, String> errors) {
        super(formatRowErrors(tableName, errors));
    }

    private static String formatRowErrors(String tableName, List<RowError> errors) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("Insertion failed at table %s for following rows: ", tableName));
        for (RowError error : errors) {
            builder.append(String.format(
                    "\n [row index %d] (Failure reason : %s) ",
                    error.getIndex(),
                    error.getMessage())
            );
        }
        return builder.toString();
    }

    private static String formatRowErrors(String tableName, Map<Integer, String> errors) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("Insertion failed at table %s for following rows: ", tableName));
        for (Map.Entry<Integer, String> error : errors.entrySet()) {
            builder.append(String.format(
                    "\n [row index %d] (Failure reason : %s) ",
                    error.getKey(),
                    error.getValue()
            ));
        }
        return builder.toString();
    }
}
