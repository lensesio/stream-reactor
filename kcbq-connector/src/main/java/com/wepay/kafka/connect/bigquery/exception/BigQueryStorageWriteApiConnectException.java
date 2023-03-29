package com.wepay.kafka.connect.bigquery.exception;


import org.apache.kafka.connect.errors.ConnectException;

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
    public BigQueryStorageWriteApiConnectException(Throwable error) {
        super(error);
    }
}
