package com.wepay.kafka.connect.bigquery.convert;


public enum KafkaSchemaRecordType {
    VALUE("value"),
    KEY("key");
    String str;
    KafkaSchemaRecordType(String str) {
        this.str = str;
    }
    public String toString() {
        return this.str;
    }
}
