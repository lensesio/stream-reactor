package com.wepay.kafka.connect.bigquery.api;


/**
 *  Enum class for Kafka schema or record type, either value or key.
 */
public enum KafkaSchemaRecordType {

    VALUE("value"),
    KEY("key");

    private final String str;

    KafkaSchemaRecordType(String str) {
        this.str = str;
    }

    public String toString() {
        return this.str;
    }
}
