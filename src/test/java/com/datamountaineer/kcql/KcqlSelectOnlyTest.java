package com.datamountaineer.kcql;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class KcqlSelectOnlyTest {

    @Test
    public void parseStartAndSetAField() {
        String topic = "TOPIC_A";
        String syntax = String.format("SELECT * FROM %s", topic);
        Kcql kcql = Kcql.parse(syntax);
        assertEquals(topic, kcql.getSource());
        assertNull(kcql.getTarget());
        assertFalse(kcql.getFields().isEmpty());
        assertTrue(kcql.isIncludeAllFields());
    }

    @Test
    public void parseASelectAllFromTopic() {
        String topic = "TOPIC_A";
        String syntax = String.format("SELECT * FROM %s withformat text", topic);
        Kcql kcql = Kcql.parse(syntax);
        assertEquals(topic, kcql.getSource());
        assertNull(kcql.getTarget());
        assertFalse(kcql.getFields().isEmpty());
        assertTrue(kcql.getFields().get(0).getName().equals("*"));
        assertTrue(kcql.isIncludeAllFields());
        HashSet<String> pks = new HashSet<>();
        Iterator<String> iter = kcql.getPrimaryKeys();
        while (iter.hasNext()) {
            pks.add(iter.next());
        }
        assertEquals(0, pks.size());
        assertNull(kcql.getConsumerGroup());
        assertNull(kcql.getSampleCount());
        assertNull(kcql.getSampleRate());
        assertEquals(FormatType.TEXT, kcql.getFormatType());
    }

    @Test
    public void parseInsertSelectWithPkNonParticipatingInFieldSelection() {
        // RDBMS KCQL should not allow this - but we need flexibility for other target systems
        String KCQL = "INSERT INTO SENSOR- SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SS";
        Kcql kcql = Kcql.parse(KCQL);
        assertEquals("SS", kcql.getStoredAs());
    }

    @Test
    public void testSELECTwithPK() {
        String KCQL = "SELECT temperature, humidity FROM sensorsTopic PK sensorID";
        Kcql kcql = Kcql.parse(KCQL);
        assertEquals("sensorID", kcql.getPrimaryKeys().next());
    }

    @Test
    public void testSTOREAS() {
        // RDBMS KCQL should not allow this - but we need flexibility for other target systems
        String KCQL = "SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SS";
        Kcql kcql = Kcql.parse(KCQL);
        assertEquals("SS", kcql.getStoredAs());
        assertEquals("sensorID", kcql.getPrimaryKeys().next());
    }

    @Test
    public void parseASelectAllFromTopicWithAConsumerGroup() {
        String topic = "TOPIC_A";
        String expectedConsumerGroup = "myconsumer-group";
        String syntax = String.format("SELECT * FROM %s withformat binary WITHGROUP %s", topic, expectedConsumerGroup);
        Kcql kcql = Kcql.parse(syntax);
        assertEquals(topic, kcql.getSource());
        assertNull(kcql.getTarget());
        assertFalse(kcql.getFields().isEmpty());
        assertTrue(kcql.getFields().get(0).getName().equals("*"));
        assertTrue(kcql.isIncludeAllFields());
        HashSet<String> pks = new HashSet<>();
        Iterator<String> iter = kcql.getPrimaryKeys();
        while (iter.hasNext()) {
            pks.add(iter.next());
        }
        assertEquals(0, pks.size());
        assertEquals(expectedConsumerGroup, kcql.getConsumerGroup());
        assertNull(kcql.getSampleCount());
        assertNull(kcql.getSampleRate());
        assertEquals(FormatType.BINARY, kcql.getFormatType());
    }

    @Test
    public void parseASelectAllFromTopicWithAConsumerGroup123() {
        String topic = "TOPIC_A";
        String expectedConsumerGroup = "123";
        String syntax = String.format("SELECT * FROM %s withformat avro WITHGROUP %s", topic, expectedConsumerGroup);
        Kcql kcql = Kcql.parse(syntax);
        assertEquals(topic, kcql.getSource());
        assertNull(kcql.getTarget());
        assertFalse(kcql.getFields().isEmpty());
        assertTrue(kcql.getFields().get(0).getName().equals("*"));
        HashSet<String> pks = new HashSet<>();
        Iterator<String> iter = kcql.getPrimaryKeys();
        while (iter.hasNext()) {
            pks.add(iter.next());
        }
        assertEquals(0, pks.size());
        assertEquals(expectedConsumerGroup, kcql.getConsumerGroup());
        assertEquals(FormatType.AVRO, kcql.getFormatType());
    }

    @Test
    public void parseASelectAllFromTopicWithMultiplePartitionsAndOffset() {
        String topic = "TOPIC_A";
        Long expectedOffset1 = 1L;
        int partition1 = 2;

        Long expectedOffset2 = 1252L;
        int partition2 = 0;

        String syntax = String.format("SELECT * FROM %s WITHFORMAT AVRO WITHOFFSET (%d,%d), (%d,%d)",
                topic, partition1, expectedOffset1, partition2, expectedOffset2);
        Kcql kcql = Kcql.parse(syntax);
        assertEquals(topic, kcql.getSource());
        assertNull(kcql.getTarget());
        assertFalse(kcql.getFields().isEmpty());
        assertTrue(kcql.getFields().get(0).getName().equals("*"));
        assertTrue(kcql.isIncludeAllFields());
        HashSet<String> pks = new HashSet<>();
        Iterator<String> iter = kcql.getPrimaryKeys();
        while (iter.hasNext()) {
            pks.add(iter.next());
        }
        assertEquals(0, pks.size());

        List<PartitionOffset> partitionOffsets = kcql.getPartitonOffset();
        assertNotNull(partitionOffsets);
        assertEquals(2, partitionOffsets.size());

        PartitionOffset po1 = partitionOffsets.get(0);

        assertEquals(partition1, po1.getPartition());
        assertEquals(expectedOffset1, po1.getOffset());

        PartitionOffset po2 = partitionOffsets.get(1);
        assertEquals(partition2, po2.getPartition());
        assertEquals(expectedOffset2, po2.getOffset());

        assertNull(kcql.getSampleCount());
        assertNull(kcql.getSampleRate());
        assertEquals(FormatType.AVRO, kcql.getFormatType());
    }

    @Test
    public void parseASelectAllFromTopicWithJustPartitionNoOffset() {
        String topic = "TOPIC_A";

        String syntax = String.format("SELECT * FROM %s withformat text WITHOFFSET (0)", topic);
        Kcql kcql = Kcql.parse(syntax);
        assertEquals(topic, kcql.getSource());
        assertNull(kcql.getTarget());
        assertTrue(kcql.getFields().get(0).getName().equals("*"));
        assertTrue(kcql.isIncludeAllFields());
        HashSet<String> pks = new HashSet<>();
        Iterator<String> iter = kcql.getPrimaryKeys();
        while (iter.hasNext()) {
            pks.add(iter.next());
        }
        assertEquals(0, pks.size());
        assertNotNull(kcql.getPartitonOffset());
        assertEquals(1, kcql.getPartitonOffset().size());
        PartitionOffset po = kcql.getPartitonOffset().get(0);
        assertEquals(0, po.getPartition());
        assertNull(po.getOffset());

        assertNull(kcql.getSampleCount());
        assertNull(kcql.getSampleRate());
    }


    @Test
    public void parseASelectWithAliasingFields() {
        String topic = "TOPIC-A";
        String syntax = String.format("SELECT f1 as col1, f2 as col2 FROM %s withformat binary", topic);
        Kcql kcql = Kcql.parse(syntax);
        assertEquals(topic, kcql.getSource());
        assertNull(kcql.getTarget());
        List<Field> fa = Lists.newArrayList(kcql.getFields());
        Map<String, Field> map = new HashMap<>();
        for (Field alias : fa) {
            map.put(alias.getName(), alias);
        }
        assertEquals(2, fa.size());
        assertTrue(map.containsKey("f1"));
        assertEquals("col1", map.get("f1").getAlias());
        assertTrue(map.containsKey("f2"));
        assertEquals("col2", map.get("f2").getAlias());
        assertFalse(kcql.isIncludeAllFields());
    }


    @Test
    public void parseASelectWithAMixOfAliasing() {
        String topic = "TOPIC.A";
        String syntax = String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM `%s` withformat text", topic);
        Kcql kcql = Kcql.parse(syntax);
        assertEquals(topic, kcql.getSource());
        assertNull(kcql.getTarget());
        List<Field> fa = Lists.newArrayList(kcql.getFields());
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
        assertFalse(kcql.isIncludeAllFields());
        assertNull(kcql.getSampleCount());
        assertNull(kcql.getSampleRate());
    }

    @Test
    public void parseASelectWithSampleRateAndSampleCount() {
        String topic = "TOPIC.A";
        Integer expectedSampleCount = 100;
        Integer expectedSampleRate = 1500;
        String syntax = String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM `%s` withformat binary SAMPLE %d EVERY %d",
                topic, expectedSampleCount, expectedSampleRate);
        Kcql kcql = Kcql.parse(syntax);
        assertEquals(topic, kcql.getSource());
        assertNull(kcql.getTarget());
        List<Field> fa = Lists.newArrayList(kcql.getFields());
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
        assertFalse(kcql.isIncludeAllFields());

        assertEquals(expectedSampleCount, kcql.getSampleCount());
        assertEquals(expectedSampleRate, kcql.getSampleRate());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTheFromOffsetIsNotAValidNumber() {
        String topic = "TOPIC.A";
        String syntax = String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM %s WITHFORMAT AVRO WITHOFFSET 11a1", topic);
        Kcql.parse(syntax);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTheSampleCountIsNotANumber() {
        String topic = "TOPIC.A";
        String syntax = String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM %s  WITHFORMAT AVRO SAMPLE a EVERY 10000", topic);
        Kcql.parse(syntax);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTheSampleCountIsZero() {
        String topic = "TOPIC.A";
        String syntax = String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM %s WITHFORMAT AVRO SAMPLE 0 EVERY 10000", topic);
        Kcql.parse(syntax);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTheSampleRateIsNotANumber() {
        String topic = "TOPIC.A";
        String syntax = String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM %s WITHFORMAT AVRO SAMPLE 10 EVERY a91", topic);
        Kcql.parse(syntax);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTheSampleRateIsZero() {
        String topic = "TOPIC.A";
        String syntax = String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM %s WITHFORMAT AVRO SAMPLE 10 EVERY 0", topic);
        Kcql.parse(syntax);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTheFormatIsNotCorrect() {
        String topic = "TOPIC.A";
        String syntax = String.format("SELECT f1 as col1, f3, f2 as col2,f4 FROM %s WITHFORMAT ARO SAMPLE 10 EVERY 0", topic);
        Kcql.parse(syntax);
    }
}
