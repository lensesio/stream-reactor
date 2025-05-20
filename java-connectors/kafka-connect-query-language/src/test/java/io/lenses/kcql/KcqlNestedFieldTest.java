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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.Test;

class KcqlNestedFieldTest {

  @Test
  void parseNestedField() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT field1.field2.field3 FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    List<Field> fields = kcql.getFields();
    assertEquals(1, fields.size());
    assertEquals("field3", fields.get(0).getName());
    assertEquals("field3", fields.get(0).getAlias());
    assertEquals(2, fields.get(0).getParentFields().size());

    assertEquals("field1", fields.get(0).getParentFields().get(0));
    assertEquals("field2", fields.get(0).getParentFields().get(1));
    assertEquals(FieldType.VALUE, fields.get(0).getFieldType());
  }

  @Test
  void parseNestedFieldWithAlias() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT field2.field3 as afield FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    List<Field> fields = kcql.getFields();
    assertEquals(1, fields.size());
    assertEquals("field3", fields.get(0).getName());
    assertEquals(1, fields.get(0).getParentFields().size());

    assertEquals("field2", fields.get(0).getParentFields().get(0));
    assertEquals("afield", fields.get(0).getAlias());
    assertEquals(FieldType.VALUE, fields.get(0).getFieldType());
  }

  @Test
  void parseMultipleFieldsOneWithAliasTheOtherWithoutNestedFieldWithAlias() {
    String topic = "TOPIC.A";
    String table = "TABLE/A";
    String syntax = String.format("INSERT INTO %s SELECT fieldA as A, field1.field2 as B FROM `%s`", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    List<Field> fields = kcql.getFields();
    assertEquals(2, fields.size());
    assertEquals("fieldA", fields.get(0).getName());
    assertEquals("A", fields.get(0).getAlias());
    assertFalse(fields.get(0).hasParents());
    assertEquals(FieldType.VALUE, fields.get(0).getFieldType());

    assertEquals("field2", fields.get(1).getName());
    assertEquals("field1", fields.get(1).getParentFields().get(0));
    assertEquals("B", fields.get(1).getAlias());
    assertEquals(FieldType.VALUE, fields.get(1).getFieldType());
  }

  @Test
  void parseNestedFieldWithStart() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT field1.field2.field3.* FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    List<Field> fields = kcql.getFields();
    assertEquals(1, fields.size());
    assertEquals("*", fields.get(0).getName());
    assertEquals("*", fields.get(0).getAlias());
    assertEquals(3, fields.get(0).getParentFields().size());
    assertEquals(FieldType.VALUE, fields.get(0).getFieldType());

    for (int i = 0; i < 3; ++i) {
      assertEquals("field" + (i + 1), fields.get(0).getParentFields().get(i));
    }
  }

  @Test
  void throwAnExceptionIfTheFieldSelectionHasStartField() {
    assertThrows(
        IllegalArgumentException.class, () -> Kcql.parse("INSERT INTO target SELECT field1.field2.*.field3 FROM source"
        )
    );
  }

  @Test
  void throwAnExceptionIfTheFieldSelectionStartsWithStar() {
    assertThrows(
        IllegalArgumentException.class, () -> Kcql.parse("INSERT INTO target SELECT *.field3 FROM source")
    );
  }

  @Test
  void throwAnExceptionIfOnlyUnderscoreIsProvided() {
    assertThrows(
        IllegalArgumentException.class, () -> Kcql.parse("INSERT INTO target SELECT _. FROM source"));
  }

  @Test
  void throwAnExceptionIfFollowingUnderscoreIsNotAMetadataOrKey() {
    assertThrows(
        IllegalArgumentException.class, () -> Kcql.parse("INSERT INTO target SELECT _.picachu FROM source"
        )
    );
  }

  @Test
  void throwAnExceptionIfNotSpecifyingAnythingWhenUsingKey() {
    assertThrows(IllegalArgumentException.class, () -> Kcql.parse("INSERT INTO target SELECT _.key FROM source"));
  }

  @Test
  void throwAnExceptionIfNotSpecifyingAnythingWhenUsingKeyAndDot() {
    assertThrows(IllegalArgumentException.class, () -> Kcql.parse("INSERT INTO target SELECT _.key. FROM source"));
  }

  @Test
  void parseKeyFields() {
    Kcql kcql = Kcql.parse("INSERT INTO target SELECT _.key.field1,_.key.field2.field3.field4, _.key.* FROM source");
    List<Field> fields = kcql.getFields();
    assertEquals(3, fields.size());

    HashMap<String, Field> fieldsMap = new HashMap<>();
    for (Field f : fields) {
      fieldsMap.put(f.getName(), f);
    }

    assertEquals(3, fieldsMap.size());

    assertTrue(fieldsMap.containsKey("field1"));
    Field field = fieldsMap.get("field1");
    assertEquals("field1", field.getName());
    assertEquals("field1", field.getAlias());
    assertFalse(field.hasParents());
    assertEquals(FieldType.KEY, field.getFieldType());

    assertTrue(fieldsMap.containsKey("*"));
    field = fieldsMap.get("*");
    assertEquals("*", field.getName());
    assertEquals("*", field.getAlias());
    assertFalse(field.hasParents());
    assertEquals(FieldType.KEY, field.getFieldType());

    assertTrue(fieldsMap.containsKey("*"));
    field = fieldsMap.get("field4");
    assertEquals("field4", field.getName());
    assertEquals("field4", field.getAlias());
    assertTrue(field.hasParents());
    assertEquals(2, field.getParentFields().size());
    assertEquals("field2", field.getParentFields().get(0));
    assertEquals("field3", field.getParentFields().get(1));
    assertEquals(FieldType.KEY, field.getFieldType());
  }

  @Test
  void throwAnExceptionIfKeySelectWithStartIsAliased() {
    assertThrows(IllegalArgumentException.class, () -> Kcql.parse(
        "INSERT INTO target SELECT _.key.field1 as F1,_.key.field2.field3.field4 as F2, _.key.* as X FROM source"));
  }

  @Test
  void parseKeyFieldsWithAlias() {
    Kcql kcql =
        Kcql.parse(
            "INSERT INTO target SELECT _.key.field1 as F1,_.key.field2.field3.field4 as F2, _.key.* FROM source");
    List<Field> fields = kcql.getFields();
    assertEquals(3, fields.size());

    HashMap<String, Field> fieldsMap = new HashMap<>();
    for (Field f : fields) {
      fieldsMap.put(f.getName(), f);
    }

    assertEquals(3, fieldsMap.size());

    assertTrue(fieldsMap.containsKey("field1"));
    Field field = fieldsMap.get("field1");
    assertEquals("field1", field.getName());
    assertEquals("F1", field.getAlias());
    assertFalse(field.hasParents());
    assertEquals(FieldType.KEY, field.getFieldType());

    assertTrue(fieldsMap.containsKey("*"));
    field = fieldsMap.get("*");
    assertEquals("*", field.getName());
    assertEquals("*", field.getAlias());
    assertFalse(field.hasParents());
    assertEquals(FieldType.KEY, field.getFieldType());

    assertTrue(fieldsMap.containsKey("*"));
    field = fieldsMap.get("field4");
    assertEquals("field4", field.getName());
    assertEquals("F2", field.getAlias());
    assertTrue(field.hasParents());
    assertEquals(2, field.getParentFields().size());
    assertEquals("field2", field.getParentFields().get(0));
    assertEquals("field3", field.getParentFields().get(1));
    assertEquals(FieldType.KEY, field.getFieldType());
  }

  @Test
  void parseMetadataFields() {
    Kcql kcql = Kcql.parse("INSERT INTO target SELECT _.topic,_.partition,_.offset, _.timestamp FROM source");
    List<Field> fields = kcql.getFields();
    assertEquals(4, fields.size());

    HashMap<String, Field> fieldsMap = new HashMap<>();
    for (Field f : fields) {
      fieldsMap.put(f.getName(), f);
    }

    assertEquals(4, fieldsMap.size());
    assertTrue(fieldsMap.containsKey("topic"));
    Field field = fieldsMap.get("topic");
    assertEquals("topic", field.getName());
    assertEquals("topic", field.getAlias());
    assertEquals(FieldType.TOPIC, field.getFieldType());

    assertTrue(fieldsMap.containsKey("partition"));
    field = fieldsMap.get("partition");
    assertEquals("partition", field.getName());
    assertEquals("partition", field.getAlias());
    assertEquals(FieldType.PARTITION, field.getFieldType());

    assertTrue(fieldsMap.containsKey("offset"));
    field = fieldsMap.get("offset");
    assertEquals("offset", field.getName());
    assertEquals("offset", field.getAlias());
    assertEquals(FieldType.OFFSET, field.getFieldType());

    assertTrue(fieldsMap.containsKey("timestamp"));
    field = fieldsMap.get("timestamp");
    assertEquals("timestamp", field.getName());
    assertEquals("timestamp", field.getAlias());
    assertEquals(FieldType.TIMESTAMP, field.getFieldType());
  }

  @Test
  void parseMetadataFieldsWithAlias() {
    Kcql kcql =
        Kcql.parse(
            "INSERT INTO target SELECT _.topic as T,_.partition as P,_.offset as O, _.timestamp as TS FROM source");
    List<Field> fields = kcql.getFields();
    assertEquals(4, fields.size());

    HashMap<String, Field> fieldsMap = new HashMap<>();
    for (Field f : fields) {
      fieldsMap.put(f.getName(), f);
    }

    assertEquals(4, fieldsMap.size());
    assertTrue(fieldsMap.containsKey("topic"));
    Field field = fieldsMap.get("topic");
    assertEquals("topic", field.getName());
    assertEquals("T", field.getAlias());
    assertEquals(FieldType.TOPIC, field.getFieldType());

    assertTrue(fieldsMap.containsKey("partition"));
    field = fieldsMap.get("partition");
    assertEquals("partition", field.getName());
    assertEquals("P", field.getAlias());
    assertEquals(FieldType.PARTITION, field.getFieldType());

    assertTrue(fieldsMap.containsKey("offset"));
    field = fieldsMap.get("offset");
    assertEquals("offset", field.getName());
    assertEquals("O", field.getAlias());
    assertEquals(FieldType.OFFSET, field.getFieldType());

    assertTrue(fieldsMap.containsKey("timestamp"));
    field = fieldsMap.get("timestamp");
    assertEquals("timestamp", field.getName());
    assertEquals("TS", field.getAlias());
    assertEquals(FieldType.TIMESTAMP, field.getFieldType());
  }

  @Test
  void parseFieldsWithTheSameNameButDifferentPath() {
    String topic = "TOPIC.A";
    String table = "TABLE/A";
    String syntax =
        String.format("INSERT INTO %s SELECT fieldA as A, field1.field2.fieldA, fieldx.fieldA as B FROM `%s`", table,
            topic);
    Kcql kcql = Kcql.parse(syntax);
    List<Field> fields = kcql.getFields();
    assertEquals(3, fields.size());
    assertEquals("fieldA", fields.get(0).getName());
    assertEquals("A", fields.get(0).getAlias());
    assertFalse(fields.get(0).hasParents());
    assertEquals(FieldType.VALUE, fields.get(0).getFieldType());

    assertEquals("fieldA", fields.get(1).getName());
    assertEquals("field1", fields.get(1).getParentFields().get(0));
    assertEquals("field2", fields.get(1).getParentFields().get(1));
    assertEquals("fieldA", fields.get(1).getAlias());
    assertEquals(FieldType.VALUE, fields.get(1).getFieldType());

    assertEquals("fieldA", fields.get(2).getName());
    assertEquals("B", fields.get(2).getAlias());
    assertTrue(fields.get(2).hasParents());
    assertEquals(1, fields.get(2).getParentFields().size());
    assertEquals("fieldx", fields.get(2).getParentFields().get(0));
    assertEquals(FieldType.VALUE, fields.get(2).getFieldType());
  }

  @Test
  void handleWithStructureQuery() {
    Kcql kcql =
        Kcql.parse("INSERT INTO pizza_avro_out\n" +
            "SELECT\n" +
            "  name,\n" +
            "  ingredients.name as fieldName,\n" +
            "  calories as cals,\n" +
            "  ingredients.sugar as fieldSugar,\n" +
            "  ingredients.*\n" +
            "FROM pizza_avro_in withstructure");
    assertTrue(kcql.hasRetainStructure());
  }

  @Test
  void parseWithIncrementalMode() {
    String incMode = "modeA";
    String syntax = "SELECT * FROM topicA INCREMENTALMODE=" + incMode;
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(incMode, kcql.getIncrementalMode());
  }

  @Test
  void parseWithIndexSuffix() {
    String syntax = "SELECT * FROM topicA WITHINDEXSUFFIX=suffix1";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("suffix1", kcql.getIndexSuffix());

    syntax = "SELECT * FROM topicA WITHINDEXSUFFIX= _{YYYY-MM-dd} ";
    kcql = Kcql.parse(syntax);
    assertEquals("_{YYYY-MM-dd}", kcql.getIndexSuffix());

    syntax = "INSERT INTO index_name SELECT * FROM topicA WITHINDEXSUFFIX= _suffix_{YYYY-MM-dd}";
    kcql = Kcql.parse(syntax);
    assertEquals("_suffix_{YYYY-MM-dd}", kcql.getIndexSuffix());
  }

  @Test
  void parseIngoreFields() {
    String syntax = "SELECT * FROM topicA ignore f1, f2.a";
    Kcql kcql = Kcql.parse(syntax);
    List<Field> ignored = kcql.getIgnoredFields();
    assertEquals(2, ignored.size());
    assertEquals("f1", ignored.get(0).getName());
    assertEquals("a", ignored.get(1).getName());
    assertEquals(1, ignored.get(1).getParentFields().size());
    assertEquals("f2", ignored.get(1).getParentFields().get(0));
  }

  @Test
  void parseWithDocType() {
    String syntax = "SELECT * FROM topicA WITHDOCTYPE=document1";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("document1", kcql.getDocType());

    syntax = "SELECT * FROM topicA WITHDOCTYPE= `document.name` ";
    kcql = Kcql.parse(syntax);
    assertEquals("document.name", kcql.getDocType());
    assertNull(kcql.getWithConverter());
  }

  @Test
  void parseWithConverter() {
    String syntax = "SELECT * FROM topicA WITHCONVERTER=`com.datamountaineer.converter.Mine`";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("com.datamountaineer.converter.Mine", kcql.getWithConverter());

    syntax = "SELECT * FROM topicA WITHCONVERTER= `com.datamountaineer.ConverterA` ";
    kcql = Kcql.parse(syntax);
    assertEquals("com.datamountaineer.ConverterA", kcql.getWithConverter());
  }

  @Test
  void parseWithJmsSelector() {
    String syntax = "INSERT INTO out SELECT * FROM topicA WITHJMSSELECTOR=`apples > 10`";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("apples > 10", kcql.getWithJmsSelector());

    syntax = "INSERT INTO out SELECT * FROM topicA WITHJMSSELECTOR= `apples > 10` ";
    kcql = Kcql.parse(syntax);
    assertEquals("apples > 10", kcql.getWithJmsSelector());
  }
}
