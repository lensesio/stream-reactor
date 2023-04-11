package com.wepay.kafka.connect.bigquery.write.storageApi;

public enum StreamState {
    CREATED,
    APPEND,
    FINALISED,
    COMMITTED,
    INACTIVE
}
