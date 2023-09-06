package com.wepay.kafka.connect.bigquery.write.storageApi;

/**
 * Enums for Stream states
 */
public enum StreamState {
    CREATED,
    APPEND,
    FINALISED,
    COMMITTED,
    INACTIVE
}
