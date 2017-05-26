package com.datamountaineer.kcql;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class KcqlNestedFieldTest {

    @Test
    public void parseNestedField() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT field1.field2.field3 FROM %s", table, topic);
        Kcql kcql = Kcql.parse(syntax);
        List<Field> fields = Lists.newArrayList(kcql.getFields());
        assertEquals(1, fields.size());
        assertEquals("field3", fields.get(0).getName());
        assertEquals("field3", fields.get(0).getAlias());
        assertEquals(2, fields.get(0).getParentFields().size());

        assertEquals("field1", fields.get(0).getParentFields().get(0));
        assertEquals("field2", fields.get(0).getParentFields().get(1));
        assertEquals(FieldType.VALUE, fields.get(0).getFieldType());
    }

    @Test
    public void parseNestedFieldWithAlias() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT field2.field3 as afield FROM %s", table, topic);
        Kcql kcql = Kcql.parse(syntax);
        List<Field> fields = Lists.newArrayList(kcql.getFields());
        assertEquals(1, fields.size());
        assertEquals("field3", fields.get(0).getName());
        assertEquals(1, fields.get(0).getParentFields().size());

        assertEquals("field2", fields.get(0).getParentFields().get(0));
        assertEquals("afield", fields.get(0).getAlias());
        assertEquals(FieldType.VALUE, fields.get(0).getFieldType());
    }

    @Test
    public void parseMultipleFieldsOneWithAliasTheOtherWithoutNestedFieldWithAlias() {
        String topic = "TOPIC.A";
        String table = "TABLE/A";
        String syntax = String.format("INSERT INTO %s SELECT fieldA as A, field1.field2 as B FROM `%s`", table, topic);
        Kcql kcql = Kcql.parse(syntax);
        List<Field> fields = Lists.newArrayList(kcql.getFields());
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
    public void parseNestedFieldWithStart() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT field1.field2.field3.* FROM %s", table, topic);
        Kcql kcql = Kcql.parse(syntax);
        List<Field> fields = Lists.newArrayList(kcql.getFields());
        assertEquals(1, fields.size());
        assertEquals("*", fields.get(0).getName());
        assertEquals("*", fields.get(0).getAlias());
        assertEquals(3, fields.get(0).getParentFields().size());
        assertEquals(FieldType.VALUE, fields.get(0).getFieldType());

        for (int i = 0; i < 3; ++i) {
            assertEquals("field" + (i + 1), fields.get(0).getParentFields().get(i));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTheFieldSelectionHasStartField() {
        Kcql.parse("INSERT INTO target SELECT field1.field2.*.field3 FROM source");
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTheFieldSelectionStartsWithStar() {
        Kcql.parse("INSERT INTO target SELECT *.field3 FROM source");
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfOnlyUnderscoreIsProvided() {
        Kcql.parse("INSERT INTO target SELECT _. FROM source");
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfFollowingUnderscoreIsNotAMetadataOrKey() {
        Kcql.parse("INSERT INTO target SELECT _.picachu FROM source");
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfNotSpecifyingAnythingWhenUsingKey() {
        Kcql.parse("INSERT INTO target SELECT _.key FROM source");
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfNotSpecifyingAnythingWhenUsingKeyAndDot() {
        Kcql.parse("INSERT INTO target SELECT _.key. FROM source");
    }

    @Test
    public void parseKeyFields() {
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

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfKeySelectWithStartIsAliased() {
        Kcql.parse("INSERT INTO target SELECT _.key.field1 as F1,_.key.field2.field3.field4 as F2, _.key.* as X FROM source");
    }

    @Test
    public void parseKeyFieldsWithAlias() {
        Kcql kcql = Kcql.parse("INSERT INTO target SELECT _.key.field1 as F1,_.key.field2.field3.field4 as F2, _.key.* FROM source");
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
    public void parseMetadataFields() {
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
    public void parseMetadataFieldsWithAlias() {
        Kcql kcql = Kcql.parse("INSERT INTO target SELECT _.topic as T,_.partition as P,_.offset as O, _.timestamp as TS FROM source");
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
    public void parseFieldsWithTheSameNameButDifferentPath() {
        String topic = "TOPIC.A";
        String table = "TABLE/A";
        String syntax = String.format("INSERT INTO %s SELECT fieldA as A, field1.field2.fieldA, fieldx.fieldA as B FROM `%s`", table, topic);
        Kcql kcql = Kcql.parse(syntax);
        List<Field> fields = Lists.newArrayList(kcql.getFields());
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
    public void handleWithStructureQuery() {
        Kcql kcql = Kcql.parse("INSERT INTO pizza_avro_out\n" +
                "SELECT\n" +
                "  name,\n" +
                "  ingredients.name as fieldName,\n" +
                "  calories as cals,\n" +
                "  ingredients.sugar as fieldSugar,\n" +
                "  ingredients.*\n" +
                "FROM pizza_avro_in withstructure");
        assertEquals(kcql.hasRetainStructure(), true);
    }


    @Test
    public void parseWithIncrementalMode() {
        String incMode = "modeA";
        String syntax = "SELECT * FROM topicA INCREMENTALMODE=" + incMode;
        Kcql kcql = Kcql.parse(syntax);
        assertEquals(incMode, kcql.getIncrementalMode());
    }

    @Test
    public void parseWithIndexSuffix() {
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
    public void parseIngoreFields() {
        String syntax = "SELECT * FROM topicA ignore f1, f2.a";
        Kcql kcql = Kcql.parse(syntax);
        List<Field> ignored = kcql.getIgnoredFields();
        assertEquals(2, ignored.size());
        assertEquals(ignored.get(0).getName(),"f1");
        assertEquals(ignored.get(1).getName(),"a");
        assertEquals(1, ignored.get(1).getParentFields().size());
        assertEquals("f2", ignored.get(1).getParentFields().get(0));
    }

    @Test
    public void parseWithDocType() {
        String syntax = "SELECT * FROM topicA WITHDOCTYPE=document1";
        Kcql kcql = Kcql.parse(syntax);
        assertEquals("document1", kcql.getDocType());

        syntax = "SELECT * FROM topicA WITHDOCTYPE= `document.name` ";
        kcql = Kcql.parse(syntax);
        assertEquals("document.name", kcql.getDocType());
        assertNull(kcql.getWithConverter());
    }

    @Test
    public void parseWithConverter() {
        String syntax = "SELECT * FROM topicA WITHCONVERTER=`com.datamountaineer.converter.Mine`";
        Kcql kcql = Kcql.parse(syntax);
        assertEquals("com.datamountaineer.converter.Mine", kcql.getWithConverter());

        syntax = "SELECT * FROM topicA WITHCONVERTER= `com.datamountaineer.ConverterA` ";
        kcql = Kcql.parse(syntax);
        assertEquals("com.datamountaineer.ConverterA", kcql.getWithConverter());
    }
}
