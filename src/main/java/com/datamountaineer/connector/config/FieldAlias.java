package com.datamountaineer.connector.config;


public class FieldAlias {
  private final String field;
  private final String alias;

  public FieldAlias(String field, String alias) {
    this.field = field;
    this.alias = alias;
  }

  public String getField() {
    return field;
  }

  public String getAlias() {
    return alias;
  }
}
