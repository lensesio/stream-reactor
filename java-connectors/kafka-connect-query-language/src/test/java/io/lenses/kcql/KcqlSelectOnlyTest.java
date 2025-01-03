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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 *
 */
class KcqlSelectOnlyTest {

  @Test
  void parseStartAndSetAField() {
    String topic = "TOPIC_A";
    String syntax = String.format("SELECT * FROM %s", topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertNull(kcql.getTarget());
    assertThat(kcql.getFields()).isNotEmpty();
  }

  @Test
  void parseASelectAllFromTopic() {
    String topic = "TOPIC_A";
    String syntax = String.format("SELECT * FROM %s withformat text", topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertNull(kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertEquals("*", kcql.getFields().get(0).getName());
    HashSet<String> pks = new HashSet<>();
    kcql.getPrimaryKeys().forEach(f -> pks.add(f.toString()));

    assertEquals(0, pks.size());
    assertEquals(FormatType.TEXT, kcql.getFormatType());
  }

  @Test
  void parseInsertSelectWithPkNonParticipatingInFieldSelection() {
    // RDBMS KCQL should not allow this - but we need flexibility for other target systems
    String KCQL = "INSERT INTO SENSOR- SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SS";
    Kcql kcql = Kcql.parse(KCQL);
    assertEquals("SS", kcql.getStoredAs());
  }

  @Test
  void testSELECTwithPK() {
    String KCQL = "SELECT temperature, humidity FROM sensorsTopic PK sensorID";
    Kcql kcql = Kcql.parse(KCQL);
    assertEquals("sensorID", kcql.getPrimaryKeys().get(0).getName());
    assertEquals("sensorID", kcql.getPrimaryKeys().get(0).getAlias());
    assertNull(kcql.getPrimaryKeys().get(0).getParentFields());
  }

  @Test
  void storeASWithAVRO() {
    String KCQL = "INSERT INTO SENSOR- SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS AVRO";
    Kcql kcql = Kcql.parse(KCQL);
    assertEquals("AVRO", kcql.getStoredAs());
  }

  @Test
  void storeASWithParquet() {
    String KCQL = "INSERT INTO SENSOR- SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS PARQUET";
    Kcql kcql = Kcql.parse(KCQL);
    assertEquals("PARQUET", kcql.getStoredAs());
  }

  @Test
  void storeASWithJSON() {
    String KCQL = "INSERT INTO SENSOR- SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS JSON";
    Kcql kcql = Kcql.parse(KCQL);
    assertEquals("JSON", kcql.getStoredAs());
  }

  @Test
  void storeASWithBYTES() {
    String KCQL = "INSERT INTO SENSOR- SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS BYTES";
    Kcql kcql = Kcql.parse(KCQL);
    assertEquals("BYTES", kcql.getStoredAs());
  }

  @Test
  void testSELECTwithNestedFieldsInPK() {
    String KCQL = "SELECT temperature, humidity FROM sensorsTopic PK metadata.sensorID, metadata.timestamp.ticks";
    Kcql kcql = Kcql.parse(KCQL);

    assertEquals(2, kcql.getPrimaryKeys().size());

    assertEquals("sensorID", kcql.getPrimaryKeys().get(0).getName());
    assertEquals("sensorID", kcql.getPrimaryKeys().get(0).getAlias());
    assertNotNull(kcql.getPrimaryKeys().get(0).getParentFields());
    assertEquals(1, kcql.getPrimaryKeys().get(0).getParentFields().size());
    assertEquals("metadata", kcql.getPrimaryKeys().get(0).getParentFields().get(0));

    assertEquals("ticks", kcql.getPrimaryKeys().get(1).getName());
    assertEquals("ticks", kcql.getPrimaryKeys().get(1).getAlias());
    assertNotNull(kcql.getPrimaryKeys().get(1).getParentFields());
    assertEquals(2, kcql.getPrimaryKeys().get(1).getParentFields().size());
    assertEquals("metadata", kcql.getPrimaryKeys().get(1).getParentFields().get(0));
    assertEquals("timestamp", kcql.getPrimaryKeys().get(1).getParentFields().get(1));
  }

  @Test
  void testSELECTwithNestedFieldsInPK2() {

    String k = "INSERT INTO index_andrew SELECT id, string_field FROM sink_test";
    Kcql kcql = Kcql.parse(k);
    assertEquals(0, kcql.getPrimaryKeys().size());

    k = "INSERT INTO index_andrew SELECT id, nested.string_field FROM sink_test";
    kcql = Kcql.parse(k);
    assertEquals(0, kcql.getPrimaryKeys().size());
    k = "UPSERT INTO sink_test SELECT id, string_field FROM sink_andrew PK id";
    kcql = Kcql.parse(k);
    assertEquals(1, kcql.getPrimaryKeys().size());

  }

  @Test
  void testSTOREAS() {
    // RDBMS KCQL should not allow this - but we need flexibility for other target systems
    String KCQL = "SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SS";
    Kcql kcql = Kcql.parse(KCQL);
    assertEquals("SS", kcql.getStoredAs());
    assertEquals("sensorID", kcql.getPrimaryKeys().get(0).getName());
    assertEquals("sensorID", kcql.getPrimaryKeys().get(0).getAlias());
    assertNull(kcql.getPrimaryKeys().get(0).getParentFields());
  }

  @Test
  void testUnwrapping() {
    // RDBMS KCQL should not allow this - but we need flexibility for other target systems
    String KCQL = "SELECT temperature, humidity FROM sensorsTopic PK sensorID WITHUNWRAP";
    Kcql kcql = Kcql.parse(KCQL);
    assertEquals("sensorID", kcql.getPrimaryKeys().get(0).getName());
    assertEquals("sensorID", kcql.getPrimaryKeys().get(0).getAlias());
    assertNull(kcql.getPrimaryKeys().get(0).getParentFields());
  }

  @Test
  void parseASelectWithAliasingFields() {
    String topic = "TOPIC-A";
    String syntax = String.format("SELECT f1 as col1, f2 as col2 FROM %s withformat binary", topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertNull(kcql.getTarget());
    List<Field> fa = kcql.getFields();
    Map<String, Field> map = new HashMap<>();
    for (Field alias : fa) {
      map.put(alias.getName(), alias);
    }
    assertEquals(2, fa.size());
    assertTrue(map.containsKey("f1"));
    assertEquals("col1", map.get("f1").getAlias());
    assertTrue(map.containsKey("f2"));
    assertEquals("col2", map.get("f2").getAlias());
  }

  @Test
  void parseASelectWithAMixOfAliasing() {
    String topic = "TOPIC.A";
    String syntax = String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM `%s` withformat text", topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertNull(kcql.getTarget());
    List<Field> fa = kcql.getFields();
    Map<String, Field> map = new HashMap<>();
    for (Field alias : fa) {
      map.put(alias.getName(), alias);
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
    assertFalse(kcql.hasRetainStructure());
  }

  @Test
  void parseASelectWithAMixOfAliasingAndUsingQuotation() {
    String topic = "TOPIC.A";
    String syntax = String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM '%s' withformat text", topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertNull(kcql.getTarget());
    List<Field> fa = kcql.getFields();
    Map<String, Field> map = new HashMap<>();
    for (Field alias : fa) {
      map.put(alias.getName(), alias);
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
    assertFalse(kcql.hasRetainStructure());
  }

  @Test
  void parseASelectWithAMixOfAliasingAndRetainStructure() {
    String topic = "TOPIC.A";
    String syntax =
        String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM `%s` withstructure withformat text", topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertNull(kcql.getTarget());
    List<Field> fa = kcql.getFields();
    Map<String, Field> map = new HashMap<>();
    for (Field alias : fa) {
      map.put(alias.getName(), alias);
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
    assertTrue(kcql.hasRetainStructure());
  }

  @Test
  void throwAnExceptionIfTheFormatIsNotCorrect() {
    String topic = "TOPIC.A";
    String syntax =
        String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM %s WITHFORMAT ARO SAMPLE 10 EVERY 0", topic);
    assertThrows(IllegalArgumentException.class, () -> Kcql.parse(syntax));
  }

  @Test
  void throwAnExceptionIfLimitNumberIsMissing() {
    String topic = "TOPIC.A";
    String syntax = String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM %s LIMIT", topic);
    assertThrows(IllegalArgumentException.class, () -> Kcql.parse(syntax));
  }

  @Test
  void throwAnExceptionIfLimitNumberIsZero() {
    String topic = "TOPIC.A";
    String syntax = String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM %s LIMIT 0", topic);
    assertThrows(IllegalArgumentException.class, () -> Kcql.parse(syntax));
  }

  @Test
  void parseLimit() {
    String topic = "TOPIC.A";
    String syntax = String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM %s LIMIT 10", topic);
    Kcql k = Kcql.parse(syntax);
    assertEquals(10, k.getLimit());
  }
}
