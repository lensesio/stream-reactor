package com.datamountaineer.kcql;

public class Tag {
  private final String key;
  private final String value;
  private final TagType type;

  public Tag(String key, String value, TagType type) {
    this.key = key;
    this.value = value;
    this.type = type;
  }

  public Tag(String key) {
    this.key = key;
    this.value = null;
    this.type = TagType.DEFAULT;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public TagType getType() {
    return type;
  }

  public enum TagType {
    DEFAULT,
    ALIAS,
    CONSTANT
  }
}
