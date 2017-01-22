package com.datamountaineer.kcql;


public enum FieldType {
    KEY("KEY"),
    OFFSET("OFFSET"),
    PARTITION("PARTITION"),
    TIMESTAMP("TIMESTAMP"),
    TOPIC("TOPIC"),
    VALUE("VALUE");

    private final String value;

    FieldType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
