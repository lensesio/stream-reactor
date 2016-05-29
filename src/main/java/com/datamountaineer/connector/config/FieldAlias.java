package com.datamountaineer.connector.config;


public class FieldAlias {
  private final String field;
  private final String alias;

  public FieldAlias(String field) {
    this(field, field);
  }

  public FieldAlias(String field, String alias) {
    if(field == null || field.trim().length()==0){
      throw new IllegalArgumentException(String.format("field is not valid:<%s>",  String.valueOf(field)));
    }
    if(alias == null || alias.trim().length()==0){
      throw new IllegalArgumentException(String.format("alias is not valid:<%s>",  String.valueOf(alias)));
    }
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
