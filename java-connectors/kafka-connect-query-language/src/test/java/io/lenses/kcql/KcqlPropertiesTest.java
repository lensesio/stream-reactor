/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.kcql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;

import org.junit.jupiter.api.Test;

class KcqlPropertiesTest {

  @Test
  void emptyPropertiesIfNotDefined() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s PK f1,f2", table, topic);
    Kcql kcql = Kcql.parse(syntax);

    assertTrue(kcql.getProperties().isEmpty());
  }

  @Test
  void emptyPropertiesIfEmpty() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s PK f1,f2 properties()", table, topic);
    Kcql kcql = Kcql.parse(syntax);

    assertTrue(kcql.getProperties().isEmpty());
  }

  @Test
  void captureThePropertiesSet() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s PK f1,f2 properties(a=23, b='xyz')", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertEquals("*", kcql.getFields().get(0).getName());
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
    HashSet<String> pks = new HashSet<>();
    kcql.getPrimaryKeys().forEach(f -> pks.add(f.toString()));

    assertEquals(2, pks.size());
    assertTrue(pks.contains("f1"));
    assertTrue(pks.contains("f2"));
    assertNull(kcql.getTags());
    assertFalse(kcql.isUnwrapping());
    assertEquals(2, kcql.getProperties().size());
    assertEquals("23", kcql.getProperties().get("a"));
    assertEquals("xyz", kcql.getProperties().get("b"));
  }

  @Test
  void capturePropertyValueWithWithSpace() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("INSERT INTO %s SELECT * FROM %s PK f1,f2 properties(a=23, b='xyz', c='a b c')", table, topic);
    Kcql kcql = Kcql.parse(syntax);

    assertEquals(3, kcql.getProperties().size());
    assertEquals("23", kcql.getProperties().get("a"));
    assertEquals("xyz", kcql.getProperties().get("b"));
    assertEquals("a b c", kcql.getProperties().get("c"));
  }

  @Test
  void handleEmptyPropertyValue() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("INSERT INTO %s SELECT * FROM %s PK f1,f2 properties(a=23, b='xyz', c='a b c', d='')", table,
            topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(4, kcql.getProperties().size());
    assertEquals("23", kcql.getProperties().get("a"));
    assertEquals("xyz", kcql.getProperties().get("b"));
    assertEquals("a b c", kcql.getProperties().get("c"));
    assertEquals("", kcql.getProperties().get("d"));
  }

  @Test
  void handlePropertyKeyWithDot() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("INSERT INTO %s SELECT * FROM %s PK f1,f2 properties(a=23, b='xyz', 'c.d'='a b c', d='')", table,
            topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(4, kcql.getProperties().size());
    assertEquals("23", kcql.getProperties().get("a"));
    assertEquals("xyz", kcql.getProperties().get("b"));
    assertEquals("a b c", kcql.getProperties().get("c.d"));
    assertEquals("", kcql.getProperties().get("d"));
  }

}
