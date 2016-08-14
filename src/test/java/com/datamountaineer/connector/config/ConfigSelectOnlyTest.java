package com.datamountaineer.connector.config;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class ConfigSelectOnlyTest {

  @Test
  public void parseASelectAllFromTopic() {
    String topic = "TOPIC_A";
    String syntax = String.format("SELECT * FROM %s", topic);
    Config config = Config.parse(syntax);
    assertEquals(topic, config.getSource());
    assertNull(config.getTarget());
    assertFalse(config.getFieldAlias().hasNext());
    assertTrue(config.isIncludeAllFields());
    HashSet<String> pks = new HashSet<>();
    Iterator<String> iter = config.getPrimaryKeys();
    while (iter.hasNext()) {
      pks.add(iter.next());
    }
    assertEquals(0, pks.size());
  }

  @Test
  public void parseASelectWithAliasingFields() {
    String topic = "TOPIC-A";
    String syntax = String.format("SELECT f1 as col1, f2 as col2 FROM %s", topic);
    Config config = Config.parse(syntax);
    assertEquals(topic, config.getSource());
    assertNull(config.getTarget());
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
  }


  @Test
  public void parseASelectWithAMixOfAliasing() {
    String topic = "TOPIC.A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f3, f2 as col2,f4 FROM %s", table, topic);
    Config config = Config.parse(syntax);
    assertEquals(topic, config.getSource());
    assertEquals(table, config.getTarget());
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
  }


}
