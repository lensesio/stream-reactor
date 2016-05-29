package com.datamountaineer.connector.config;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.testng.collections.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
  public void parseAnInsertWithFieldAlias() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2 FROM %s", table, topic);
    Config config = Config.parse(syntax);
    assertEquals(topic, config.getTopic());
    assertEquals(table, config.getTable());
    List<FieldAlias> fa = Lists.newArrayList(config.getFieldAlias());
    Map<String, FieldAlias> map = new HashMap<>();
    for (FieldAlias alias : fa) {
      map.put(alias.getField(), alias);
    }
    assertEquals(2, fa.size());
    assertTrue(map.containsKey("f1"));
    assertEquals("col1", map.get("f1").getAlias());
    assertTrue(map.containsKey("f2"));
    assertEquals("col2", map.get("f2").getAlias());
    assertFalse(config.isIncludeAllFields());
    assertEquals(WriteModeEnum.INSERT, config.getWriteMode());
  }

  @Test
  public void parseAnInsertWithFieldAliasMixedWithNoAliasing() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f3, f2 as col2,f4 FROM %s", table, topic);
    Config config = Config.parse(syntax);
    assertEquals(topic, config.getTopic());
    assertEquals(table, config.getTable());
    List<FieldAlias> fa = Lists.newArrayList(config.getFieldAlias());
    Map<String, FieldAlias> map = new HashMap<>();
    for (FieldAlias alias : fa) {
      map.put(alias.getField(), alias);
    }
    assertEquals(4, fa.size());
    assertTrue(map.containsKey("f1"));
    assertEquals("col1", map.get("f1").getAlias());
    assertTrue(map.containsKey("f2"));
    assertEquals("col2", map.get("f2").getAlias());
    assertTrue(map.containsKey("f3"));
    assertEquals("f3", map.get("f3").getAlias());
    assertTrue(map.containsKey("f4"));
    assertEquals("f4", map.get("f4").getAlias());
    assertFalse(config.isIncludeAllFields());
    assertEquals(WriteModeEnum.INSERT, config.getWriteMode());
  }

  @Test
  public void parseAnInsertWithFieldAliasMixedWithAllFieldsTheAsterixAtTheEnd() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, * FROM %s", table, topic);
    Config config = Config.parse(syntax);
    assertEquals(topic, config.getTopic());
    assertEquals(table, config.getTable());
    List<FieldAlias> fa = Lists.newArrayList(config.getFieldAlias());
    Map<String, FieldAlias> map = new HashMap<>();
    for (FieldAlias alias : fa) {
      map.put(alias.getField(), alias);
    }
    assertEquals(1, fa.size());
    assertTrue(map.containsKey("f1"));
    assertEquals("col1", map.get("f1").getAlias());
    assertTrue(config.isIncludeAllFields());
    assertEquals(WriteModeEnum.INSERT, config.getWriteMode());
  }

  @Test
  public void parseAnInsertWithFieldAliasMixedWithAllFieldsTheAsterixAtTheBegining() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT *,f1 as col1 FROM %s", table, topic);
    Config config = Config.parse(syntax);
    assertEquals(topic, config.getTopic());
    assertEquals(table, config.getTable());
    List<FieldAlias> fa = Lists.newArrayList(config.getFieldAlias());
    Map<String, FieldAlias> map = new HashMap<>();
    for (FieldAlias alias : fa) {
      map.put(alias.getField(), alias);
    }
    assertEquals(1, fa.size());
    assertTrue(map.containsKey("f1"));
    assertEquals("col1", map.get("f1").getAlias());
    assertTrue(config.isIncludeAllFields());
    assertEquals(WriteModeEnum.INSERT, config.getWriteMode());
  }

  @Test
  public void parseAnInsertWithFieldAliasMixedWithAllFieldsTheAsterixInTheMiddle() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f2 as col2,*,f1 as col1 FROM %s", table, topic);
    Config config = Config.parse(syntax);
    assertEquals(topic, config.getTopic());
    assertEquals(table, config.getTable());
    List<FieldAlias> fa = Lists.newArrayList(config.getFieldAlias());
    Map<String, FieldAlias> map = new HashMap<>();
    for (FieldAlias alias : fa) {
      map.put(alias.getField(), alias);
    }
    assertEquals(2, fa.size());
    assertTrue(map.containsKey("f1"));
    assertEquals("col1", map.get("f1").getAlias());
    assertTrue(map.containsKey("f2"));
    assertEquals("col2", map.get("f2").getAlias());
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
  public void parseAnInsertWithSelectAllFieldsWithIgnoredColumns() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s IGNORE col1 , col2 ", table, topic);
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
  public void parseAnUpsertWithSelectAllFieldsWithIgnoredColumns() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT * FROM %s IGNORE col1, 1col2  ", table, topic);
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
    assertTrue(ignored.contains("1col2"));
  }
}
