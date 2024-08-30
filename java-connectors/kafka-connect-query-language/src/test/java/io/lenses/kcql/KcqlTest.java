/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.kcql;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
class KcqlTest {

  @Test
  void parseAnInsertWithSelectAllFieldsAndNoIgnoreAndPKs() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s PK f1,f2", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertEquals("*", kcql.getFields().get(0).getName());
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());

    Set<String> pks = getPrimaryKeys(kcql);

    assertEquals(2, pks.size());
    assertTrue(pks.contains("f1"));
    assertTrue(pks.contains("f2"));
    assertNull(kcql.getTags());
    assertFalse(kcql.isUnwrapping());
  }

  @Test
  void parseSimpleSelectCommand() {
    String syntax = "SELECT * FROM topicA";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("topicA", kcql.getSource());
  }

  @Test
  void parseSimpleSelectCommandWithPK() {
    String syntax = "SELECT * FROM topicA PK lastName";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("topicA", kcql.getSource());
  }

  @Test
  void parseAnotherSimpleSelectCommandWithPK() {
    String syntax = "SELECT firstName, lastName as surname FROM topicA";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("topicA", kcql.getSource());
    assertEquals("lastName", kcql.getFields().get(1).getName());
    assertEquals("surname", kcql.getFields().get(1).getAlias());
  }

  @Test
  void parseAnInsertWithSelectAllFieldsAndNoIgnore() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertEquals("*", kcql.getFields().get(0).getName());
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  void handleTargetAndSourceContainingDot() {
    String topic = "TOPIC.A";
    String table = "TABLE.A";
    String syntax = String.format("INSERT INTO `%s` SELECT * FROM `%s`", table, topic);

    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertEquals("*", kcql.getFields().get(0).getName());
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  void handleTargetAndSourceContainingDash() {
    String topic = "TOPIC-A";
    String table = "TABLE-A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertEquals("*", kcql.getFields().get(0).getName());

    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  void parseAnInsertWithFieldAlias() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2 FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());

    assertContainsFields(
        kcql,
        new Field("f1", "col1", FieldType.VALUE),
        new Field("f2", "col2", FieldType.VALUE)
    );

  }

  @Test
  void parseAnInsertWithFieldAliasAndSettingTheBatchSize() {
    String topic = "TOPIC-A";
    String table = "TABLE_A";
    String batchSize = "500";
    String syntax =
        String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2 FROM %s BATCH = %s", table, topic, batchSize);
    Kcql kcql = Kcql.parse(syntax);

    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());

    assertContainsFields(
        kcql,
        new Field("f1", "col1", FieldType.VALUE),
        new Field("f2", "col2", FieldType.VALUE)
    );

    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
    assertEquals(500, kcql.getBatchSize());
  }

  @Test
  void parseAnInsertWithFieldAliasMixedWithNoAliasing() {
    String topic = "TOPIC.A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f3, f2 as col2,f4 FROM `%s`", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());

    assertContainsFields(
        kcql,
        new Field("f1", "col1", FieldType.VALUE),
        new Field("f2", "col2", FieldType.VALUE),
        new Field("f3", "f3", FieldType.VALUE),
        new Field("f4", "f4", FieldType.VALUE)
    );

    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  void parseAnInsertWithFieldAliasMixedWithAllFieldsTheAsterixAtTheEnd() {
    String topic = "TOPIC+A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, * FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());

    assertContainsFields(
        kcql,
        new Field("f1", "col1", FieldType.VALUE),
        new Field("*", "*", FieldType.VALUE)
    );

    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  void parseAnInsertWithDottedTarget() {
    String topic = "TOPIC+A";
    String table = "KEYSPACE.A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, * FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
  }

  @Test
  void parseAnInsertWithFieldAliasMixedWithAllFieldsTheAsterixAtTheBegining() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT *,f1 as col1 FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());

    assertContainsFields(
        kcql,
        new Field("f1", "col1", FieldType.VALUE),
        new Field("*", "*", FieldType.VALUE)
    );
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  void parseAnInsertWithFieldAliasMixedWithAllFieldsTheAsterixInTheMiddle() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f2 as col2,*,f1 as col1 FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());

    assertContainsFields(
        kcql,
        new Field("f1", "col1", FieldType.VALUE),
        new Field("f2", "col2", FieldType.VALUE),
        new Field("*", "*", FieldType.VALUE)
    );

    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  void parseAnUpsertWithSelectAllFieldsAndNoIgnore() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT * FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertEquals("*", kcql.getFields().get(0).getName());
    assertEquals(WriteModeEnum.UPSERT, kcql.getWriteMode());
  }

  @Test
  void parseAnInsertWithSelectAllFieldsWithIgnoredColumns() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s IGNORE col1 , col2 ", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertEquals("*", kcql.getFields().get(0).getName());
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
    List<Field> ignored = kcql.getIgnoredFields();

    assertEquals("col1", ignored.get(0).getName());
    assertEquals("col2", ignored.get(1).getName());

  }

  @Test
  void parseAnUpsertWithSelectAllFieldsWithIgnoredColumns() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT * FROM %s IGNORE col1, 1col2  ", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertEquals("*", kcql.getFields().get(0).getName());

    assertEquals(WriteModeEnum.UPSERT, kcql.getWriteMode());

    List<Field> ignored = kcql.getIgnoredFields();
    assertEquals("col1", ignored.get(0).getName());
    assertEquals("1col2", ignored.get(1).getName());
  }

  @Test
  void parseAnInsertWithFieldAliasAndAutocreateNoPKs() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2 FROM %s AUTOCREATE", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());

    assertContainsFields(
        kcql,
        new Field("f1", "col1", FieldType.VALUE),
        new Field("f2", "col2", FieldType.VALUE)
    );
    assertTrue(kcql.isAutoCreate());
    assertTrue(kcql.getPrimaryKeys().isEmpty());
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  void parseAnInsertWithFieldAliasAndAutocreateWithPKs() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2, col3 FROM %s AUTOCREATE PK col1,col3", table,
            topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());

    assertContainsFields(
        kcql,
        new Field("f1", "col1", FieldType.VALUE),
        new Field("f2", "col2", FieldType.VALUE),
        new Field("col3", "col3", FieldType.VALUE)
    );

    Set<String> pks = getPrimaryKeys(kcql);

    assertEquals(2, pks.size());
    assertTrue(pks.contains("col1"));
    assertTrue(pks.contains("col3"));
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());

    assertFalse(kcql.isAutoEvolve());
  }

  @Test
  void parseAnInsertWithFieldAliasAndAutocreateWithPKsAndAutoevolve() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2, col3 FROM %s AUTOCREATE PK col1,col3 AUTOEVOLVE",
            table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());

    assertContainsFields(
        kcql,
        new Field("f1", "col1", FieldType.VALUE),
        new Field("f2", "col2", FieldType.VALUE),
        new Field("col3", "col3", FieldType.VALUE)
    );
    assertTrue(kcql.isAutoCreate());

    Set<String> pks = getPrimaryKeys(kcql);

    assertEquals(2, pks.size());
    assertTrue(pks.contains("col1"));
    assertTrue(pks.contains("col3"));
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());

    assertTrue(kcql.isAutoEvolve());
  }

  @Test
  void handleTimestampAsOneOfTheFields() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s WITHTIMESTAMP col1", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("col1", kcql.getTimestamp());
  }

  @Test
  void handleTypeAsOneOfTheFields() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s WITHTYPE QUEUE", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("QUEUE", kcql.getWithType());
  }

  @Test
  void handleCompoundWITHFields() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format(
            "INSERT INTO %s SELECT col1,col2 FROM %s WITHTYPE QUEUE WITHCONVERTER=`com.blah.Converter` WITHJMSSELECTOR=`apples > 10`",
            table, topic);

    Kcql kcql = Kcql.parse(syntax);
    assertEquals("QUEUE", kcql.getWithType());
    assertEquals("com.blah.Converter", kcql.getWithConverter());
    assertEquals("apples > 10", kcql.getWithJmsSelector());
  }

  @Test
  void handleTimestampWhenAllFieldIncluded() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s WITHTIMESTAMP col1", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("col1", kcql.getTimestamp());
  }

  @Test
  void handleTimestampSetAsCurrentSysWhenAllFieldsIncluded() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s WITHTIMESTAMP " + Kcql.TIMESTAMP, table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(Kcql.TIMESTAMP, kcql.getTimestamp());
  }

  @Test
  void handleFieldSelectionWithPKWithTimestampSetAsFieldNotInSelection() {
    String syntax =
        "INSERT INTO measurements SELECT actualTemperature, targetTemperature FROM TOPIC_A PK machineId, type WITHTIMESTAMP ts";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("ts", kcql.getTimestamp());

    Set<String> pks = getPrimaryKeys(kcql);

    assertEquals(2, pks.size());
    assertTrue(pks.contains("type"));
    assertTrue(pks.contains("machineId"));
  }

  @Test
  void handleTimestampSetAsCurrentSysWhenSelectedFieldsIncluded() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("INSERT INTO %s SELECT col1, col2,col3 FROM %s WITHTIMESTAMP " + Kcql.TIMESTAMP, table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(Kcql.TIMESTAMP, kcql.getTimestamp());
  }

  @Test
  void handleAtCharacterInFields() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("INSERT INTO %s SELECT @col1, col2,col3 FROM %s WITHTIMESTAMP " + Kcql.TIMESTAMP, table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("@col1", kcql.getFields().get(0).getName());
  }

  @Test
  void handleKeyDelimeter() {
    String syntax = "INSERT INTO abc SELECT @col1, col2,col3 FROM %s KEYDELIMITER ='|'";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("|", kcql.getKeyDelimeter());
  }

  @Test
  void handleKeyDelimeterSelect() {
    String syntax = "SELECT @col1, col2,col3 FROM topic KEYDELIMITER ='|'";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("|", kcql.getKeyDelimeter());
  }

  @Test
  void handleWithKey() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("INSERT INTO %s SELECT @col1, col2,col3 FROM %s WITHKEY(col1, col2, col3)", topic, table);
    Kcql kcql = Kcql.parse(syntax);
    List<String> withKeys = kcql.getWithKeys();
    assertEquals("col1", withKeys.get(0));
    assertEquals("col2", withKeys.get(1));
    assertEquals("col3", withKeys.get(2));
    assertEquals(3, withKeys.size());
  }

  @Test
  void handleWithKeyEscaped() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("INSERT INTO %s SELECT @col1, col2,col3 FROM %s WITHKEY(`col1`, `col2`)", topic, table);
    Kcql kcql = Kcql.parse(syntax);
    List<String> withKeys = kcql.getWithKeys();
    assertEquals("col1", withKeys.get(0));
    assertEquals("col2", withKeys.get(1));
    assertEquals(2, withKeys.size());
  }

  @Test
  void handleStoredAs() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s WITHFORMAT avro", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("AVRO", kcql.getFormatType().toString());

    String syntax2 = String.format("INSERT INTO %s SELECT * FROM %s WITHFORMAT json", table, topic);
    Kcql c2 = Kcql.parse(syntax2);
    assertEquals("JSON", c2.getFormatType().toString());

    String syntax3 = String.format("INSERT INTO %s SELECT * FROM %s WITHFORMAT map", table, topic);
    Kcql c3 = Kcql.parse(syntax3);
    assertEquals("MAP", c3.getFormatType().toString());

    String syntax4 = String.format("INSERT INTO %s SELECT * FROM %s WITHFORMAT object", table, topic);
    Kcql c4 = Kcql.parse(syntax4);
    assertEquals("OBJECT", c4.getFormatType().toString());

    String syntax5 = String.format("INSERT INTO %s SELECT * FROM %s WITHFORMAT protobuf", table, topic);
    Kcql c5 = Kcql.parse(syntax5);
    assertEquals("PROTOBUF", c5.getFormatType().toString());
  }

  @Test
  void throwExceptionIfStoredAsTypeIsMissing() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s STOREAS", table, topic);
    assertThrows(IllegalArgumentException.class, () -> Kcql.parse(syntax));
  }

  @Test
  void throwAnExceptionIfStoredAsParametersIsEmpty() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s STOREAS SS ()", table, topic);
    assertThrows(IllegalArgumentException.class, () -> Kcql.parse(syntax));
  }

  @Test
  void throwAnExceptionIfStoredAsParameterAppersTwice() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("UPSERT INTO %s SELECT col1,col2 FROM %s STOREAS SS (name = something , NaMe= something)", table,
            topic);
    assertThrows(IllegalArgumentException.class, () -> Kcql.parse(syntax));
  }

  @Test
  void handleStoredAsClause() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format(
            "INSERT INTO %s SELECT col1,col2 FROM %s STOREAS SS (param1 = value1 , param2 = value2,param3=value3)",
            table, topic);

    Kcql kcql = Kcql.parse(syntax);
    assertEquals("SS", kcql.getStoredAs());
    assertEquals(3, kcql.getStoredAsParameters().size());
    assertEquals("value1", kcql.getStoredAsParameters().get("param1"));
    assertEquals("value2", kcql.getStoredAsParameters().get("param2"));
    assertEquals("value3", kcql.getStoredAsParameters().get("param3"));
  }

  @Test
  void handleSemicolonInTarget() {
    String topic = "TOPIC_A";
    String table = "namespace1:TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s", table, topic);

    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
  }

  @Test
  void handleForwardSlashInSource() {
    String topic = "/TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s", table, topic);

    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
  }

  @Test
  void handleTimestampUnit() {
    String topic = "/TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s TIMESTAMPUNIT=SECONDS", table, topic);

    Kcql kcql = Kcql.parse(syntax);
    assertEquals(TimeUnit.SECONDS, kcql.getTimestampUnit());
  }

  @Test
  void handleWithTarget() {
    String topic = "/TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("INSERT INTO %s SELECT col1,col2 FROM %s WITHTARGET = field1.field2.field3 WITHFORMAT object",
            table, topic);

    Kcql kcql = Kcql.parse(syntax);
    assertEquals("field1.field2.field3", kcql.getDynamicTarget());
  }

  @Test
  void parseTags() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format(
            "INSERT INTO %s SELECT * FROM %s WITHTAG (field1, c1=v1, field2, c2=v2, field1.field2 as namedTag)", table,
            topic);

    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertEquals("*", kcql.getFields().get(0).getName());
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());

    assertThat(getTagsMap(kcql)).hasSize(5).containsAllEntriesOf(Map.of(
        "field1", new Tag("field1", null, Tag.TagType.DEFAULT),
        "field2", new Tag("field2", null, Tag.TagType.DEFAULT),
        "c2", new Tag("c2", "v2", Tag.TagType.CONSTANT),
        "c1", new Tag("c1", "v1", Tag.TagType.CONSTANT),
        "field1.field2", new Tag("field1.field2", "namedTag", Tag.TagType.ALIAS)
    ));

  }

  private static Map<String, Tag> getTagsMap(Kcql kcql) {
    return kcql.getTags().stream()
        .collect(Collectors.toMap(Tag::getKey, tag -> tag));
  }

  @Test
  void parseTagsWithNestedFields() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("INSERT INTO %s SELECT * FROM %s WITHTAG (field1.fieldA, c1=v1, field2, c2=v2)", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertEquals("*", kcql.getFields().get(0).getName());
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());

    Map<String, Tag> tagsMap =
        getTagsMap(kcql);

    assertThat(tagsMap).hasSize(4).containsAllEntriesOf(Map.of(
        "field1.fieldA", new Tag("field1.fieldA", null, Tag.TagType.DEFAULT),
        "field2", new Tag("field2", null, Tag.TagType.DEFAULT),
        "c2", new Tag("c2", "v2", Tag.TagType.CONSTANT),
        "c1", new Tag("c1", "v1", Tag.TagType.CONSTANT)
    ));
  }

  @Test
  void throwExceptionWhenTagsWithNestedFieldsEndsWithDot() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("INSERT INTO %s SELECT * FROM %s WITHTAG (field1.fieldA., c1=v1, field2, c2=v2)", table, topic);

    assertThrows(IllegalArgumentException.class, () -> Kcql.parse(syntax));
  }

  @Test
  void throwAnExceptionIfTagsListIsEmpty() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s WITHTAGS ()", table, topic);

    Kcql kcql = Kcql.parse(syntax);
    assertNull(kcql.getTags());
  }

  @Test
  void handleWithPipeline() {
    String topic = "/TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format("INSERT INTO %s SELECT col1,col2 FROM %s WITHPIPELINE = field1.field2.field3", table, topic);

    Kcql kcql = Kcql.parse(syntax);
    assertEquals("field1.field2.field3", kcql.getPipeline());
  }

  @Test
  void handleWithSubscription() {
    String syntax = "INSERT INTO A SELECT * FROM B WITHSUBSCRIPTION = shared";

    Kcql kcql = Kcql.parse(syntax);
    assertEquals("shared", kcql.getWithSubscription());
  }

  @Test
  void handleWithRegex() {
    String topic = "/TOPIC_A";
    String table = "TABLE_A";
    String syntax =
        String.format(
            "INSERT INTO %s SELECT col1,col2 FROM %s WITHCONVERTER=`com.blah.Converter` WITHREGEX=`/^#?([a-f0-9]{6}|[a-f0-9]{3})$/`",
            table, topic);

    Kcql kcql = Kcql.parse(syntax);
    assertEquals("/^#?([a-f0-9]{6}|[a-f0-9]{3})$/", kcql.getWithRegex());
    assertEquals("com.blah.Converter", kcql.getWithConverter());
  }

  @Test
  void handleTTL() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s TTL=1", table, topic);

    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(1, kcql.getTTL());
  }

  @Test
  void handleTTLSelectOnly() {
    String table = "TABLE_A";
    String syntax = String.format("SELECT * FROM %sPK sensorID STOREAS SortedSet(score=ts) TTL = 60", table);

    Kcql kcql = Kcql.parse(syntax);
    assertEquals(60, kcql.getTTL());
  }

  @Test
  void handleLimit() {
    String syntax = "insert into mytopic select a from mytable limit 200";

    Kcql kcql = Kcql.parse(syntax);
    assertEquals(200, kcql.getLimit());
  }

  @Test
  void handleUpdate() {
    String syntax = "update into mytopic select a, b, c from topic";

    Kcql kcql = Kcql.parse(syntax);
    assertEquals(WriteModeEnum.UPDATE, kcql.getWriteMode());
  }

  @Test
  void handleKeys() {
    String syntax = "insert into target select _key.a, _key.p.c, value_field, _header.h FROM topic";

    Kcql kcql = Kcql.parse(syntax);
    assertEquals(2, kcql.getKeyFields().size());
    assertEquals("a", kcql.getKeyFields().get(0).getName());
    assertEquals("c", kcql.getKeyFields().get(1).getName());
    assertEquals("p", kcql.getKeyFields().get(1).getParentFields().get(0));
    assertEquals("h", kcql.getHeaderFields().get(0).getName());
    assertEquals("value_field", kcql.getFields().get(0).getName());
  }

  @Test
  void handleKeysAll() {
    String syntax = "insert into target select _key.* FROM topic";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(1, kcql.getKeyFields().size());
  }

  @Test
  void handleHeaders() {
    String syntax = "insert into target select _header.a, _header.p.c, value_field FROM topic";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(2, kcql.getHeaderFields().size());
    assertEquals("a", kcql.getHeaderFields().get(0).getName());
    assertEquals("c", kcql.getHeaderFields().get(1).getName());
    assertEquals("p", kcql.getHeaderFields().get(1).getParentFields().get(0));
    assertEquals("value_field", kcql.getFields().get(0).getName());
  }

  private static Set<String> getPrimaryKeys(Kcql kcql) {
    return kcql.getPrimaryKeys()
        .stream().map(Field::getName).collect(Collectors.toSet());
  }

  private static void assertContainsFields(Kcql kcql, Field... fields) {
    assertThat(kcql.getFields()).hasSize(fields.length).containsExactlyInAnyOrder(
        fields
    );
  }

}
