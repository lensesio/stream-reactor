package com.datamountaineer.connector.config;

import com.google.common.collect.Iterables;
import org.junit.Test;
import org.testng.collections.Sets;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class ConfigTest {
  @Test
  public void parseAnInsertWithSelectAllFieldsAndNoIgnore() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s", table, topic);
    Config config = Config.parse(syntax);
    assertEquals(topic, config.getTopic());
    assertEquals(table, config.getTable());
    assertFalse(config.getFieldAlias().hasNext());
    assertTrue(config.isIncludeAllFields());
    assertEquals(WriteModeEnum.INSERT, config.getWriteMode());
  }

  @Test
  public void parseAnUpsertWithSelectAllFieldsAndNoIgnore() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT * FROM %s", table, topic);
    Config config = Config.parse(syntax);
    assertEquals(topic, config.getTopic());
    assertEquals(table, config.getTable());
    assertFalse(config.getFieldAlias().hasNext());
    assertTrue(config.isIncludeAllFields());
    assertEquals(WriteModeEnum.UPSERT, config.getWriteMode());
  }

  @Test
  public void parseAnInsertWithSelectAllFieldsAndWithIgnoredColumns() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s IGNORE col1, col2", table, topic);
    Config config = Config.parse(syntax);
    assertEquals(topic, config.getTopic());
    assertEquals(table, config.getTable());
    assertFalse(config.getFieldAlias().hasNext());
    assertTrue(config.isIncludeAllFields());
    assertEquals(WriteModeEnum.INSERT, config.getWriteMode());
    Set<String> ignored = new HashSet<>();
    Iterator<String> iter = config.getIgnoredField();
    while (iter.hasNext()) {
      ignored.add(iter.next());
    }

    assertTrue(ignored.contains("col1"));
    assertTrue(ignored.contains("col2"));
  }

  @Test
  public void parseAnUpsertWithSelectAllFieldsAndNoIgnoreWithIgnoredColumns() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT * FROM %s IGNORE col1,col2", table, topic);
    Config config = Config.parse(syntax);
    assertEquals(topic, config.getTopic());
    assertEquals(table, config.getTable());
    assertFalse(config.getFieldAlias().hasNext());
    assertTrue(config.isIncludeAllFields());
    assertEquals(WriteModeEnum.UPSERT, config.getWriteMode());
    Set<String> ignored = new HashSet<>();
    Iterator<String> iter = config.getIgnoredField();
    while (iter.hasNext()) {
      ignored.add(iter.next());
    }

    assertTrue(ignored.contains("col1"));
    assertTrue(ignored.contains("col2"));
  }
}
