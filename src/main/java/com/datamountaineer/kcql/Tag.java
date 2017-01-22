package com.datamountaineer.kcql;

public class Tag {
    private final String key;
    private final String value;
    private final boolean isConstant;

    public Tag(String key, String value) {
        this.key = key;
        this.value = value;
        this.isConstant = true;
    }

    public Tag(String key) {
        this.key = key;
        this.value = null;
        this.isConstant = false;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public boolean isConstant() {
        return isConstant;
    }
}
