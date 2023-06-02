package com.datamountaineer.kcql;

import org.junit.Test;

import java.util.HashSet;

import static org.junit.Assert.*;

/**
 *
 */
public class KcqlPropertiesTest {

    @Test
    public void emptyPropertiesIfNotDefined(){
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s PK f1,f2", table, topic);
        Kcql kcql = Kcql.parse(syntax);

        assertTrue(kcql.getProperties().isEmpty());
    }

    @Test
    public void emptyPropertiesIfEmpty(){
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s PK f1,f2 properties()", table, topic);
        Kcql kcql = Kcql.parse(syntax);

        assertTrue(kcql.getProperties().isEmpty());
    }

    @Test
    public void captureThePropertiesSet() {
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
    public void capturePropertyValueWithWithSpace(){
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s PK f1,f2 properties(a=23, b='xyz', c='a b c')", table, topic);
        Kcql kcql = Kcql.parse(syntax);

        assertEquals(3, kcql.getProperties().size());
        assertEquals("23", kcql.getProperties().get("a"));
        assertEquals("xyz", kcql.getProperties().get("b"));
        assertEquals("a b c", kcql.getProperties().get("c"));
    }

    @Test
    public void handleEmptyPropertyValue(){
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s PK f1,f2 properties(a=23, b='xyz', c='a b c', d='')", table, topic);
        Kcql kcql = Kcql.parse(syntax);
        assertEquals(4, kcql.getProperties().size());
        assertEquals("23", kcql.getProperties().get("a"));
        assertEquals("xyz", kcql.getProperties().get("b"));
        assertEquals("a b c", kcql.getProperties().get("c"));
        assertEquals("", kcql.getProperties().get("d"));
    }

    @Test
    public void handlePropertyKeyWithDot(){
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s PK f1,f2 properties(a=23, b='xyz', 'c.d'='a b c', d='')", table, topic);
        Kcql kcql = Kcql.parse(syntax);
        assertEquals(4, kcql.getProperties().size());
        assertEquals("23", kcql.getProperties().get("a"));
        assertEquals("xyz", kcql.getProperties().get("b"));
        assertEquals("a b c", kcql.getProperties().get("c.d"));
        assertEquals("", kcql.getProperties().get("d"));
    }
}
