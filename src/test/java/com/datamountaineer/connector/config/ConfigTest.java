package com.datamountaineer.connector.config;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/**
 *
 */
public class ConfigTest {

    @Test
    public void parseAnInsertWithSelectAllFieldsAndNoIgnoreAndPKs() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s PK f1,f2", table, topic);
        Config config = Config.parse(syntax);
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
        assertFalse(config.getFieldAlias().hasNext());
        assertTrue(config.isIncludeAllFields());
        assertEquals(WriteModeEnum.INSERT, config.getWriteMode());
        HashSet<String> pks = new HashSet<>();
        Iterator<String> iter = config.getPrimaryKeys();
        while (iter.hasNext()) {
            pks.add(iter.next());
        }
        assertEquals(2, pks.size());
        assertTrue(pks.contains("f1"));
        assertTrue(pks.contains("f2"));
        assertFalse(config.getTags().hasNext());
        assertFalse(config.isWithUnwrap());
    }

    @Test
    public void parseSimpleSelectCommand() {
        String syntax = "SELECT * FROM topicA";
        Config config = Config.parse(syntax);
        assertEquals("topicA", config.getSource());
    }

    @Test
    public void parseSimpleSelectCommandWithPK() {
        String syntax = "SELECT * FROM topicA PK lastName";
        Config config = Config.parse(syntax);
        assertEquals("topicA", config.getSource());
    }

    @Test
    public void parseWithIncrementalMode() {
        String incMode = "modeA";
        String syntax = "SELECT * FROM topicA INCREMENTALMODE=" + incMode;
        Config config = Config.parse(syntax);
        assertEquals(incMode, config.getIncrementalMode());
    }

    @Test
    public void parseWithIndexSuffix() {
        String syntax = "SELECT * FROM topicA WITHINDEXSUFFIX=suffix1";
        Config config = Config.parse(syntax);
        assertEquals("suffix1", config.getIndexSuffix());

        syntax = "SELECT * FROM topicA WITHINDEXSUFFIX= _{YYYY-MM-dd} ";
        config = Config.parse(syntax);
        assertEquals("_{YYYY-MM-dd}", config.getIndexSuffix());

        syntax = "INSERT INTO index_name SELECT * FROM topicA WITHINDEXSUFFIX= _suffix_{YYYY-MM-dd}";
        config = Config.parse(syntax);
        assertEquals("_suffix_{YYYY-MM-dd}", config.getIndexSuffix());
    }

    @Test
    public void parseWithDocType() {
        String syntax = "SELECT * FROM topicA WITHDOCTYPE=document1";
        Config config = Config.parse(syntax);
        assertEquals("document1", config.getDocType());

        syntax = "SELECT * FROM topicA WITHDOCTYPE= document.name ";
        config = Config.parse(syntax);
        assertEquals("document.name", config.getDocType());
        assertNull(config.getWithConverter());
    }

    @Test
    public void parseWithConverter() {
        String syntax = "SELECT * FROM topicA WITHCONVERTER=com.datamountaineer.converter.Mine";
        Config config = Config.parse(syntax);
        assertEquals("com.datamountaineer.converter.Mine", config.getWithConverter());

        syntax = "SELECT * FROM topicA WITHCONVERTER= com.datamountaineer.ConverterA ";
        config = Config.parse(syntax);
        assertEquals("com.datamountaineer.ConverterA", config.getWithConverter());
    }

    @Test
    public void parseAnotherSimpleSelectCommandWithPK() {
        String syntax = "SELECT firstName, lastName as surname FROM topicA";
        Config config = Config.parse(syntax);
        assertEquals("topicA", config.getSource());
    }

    @Test
    public void parseAnInsertWithSelectAllFieldsAndNoIgnore() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s", table, topic);
        Config config = Config.parse(syntax);
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
        assertFalse(config.getFieldAlias().hasNext());
        assertTrue(config.isIncludeAllFields());
        assertEquals(WriteModeEnum.INSERT, config.getWriteMode());
    }

    @Test
    public void handleTargetAndSourceContainingDot() {
        String topic = "TOPIC.A";
        String table = "TABLE.A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s", table, topic);
        Config config = Config.parse(syntax);
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
        assertFalse(config.getFieldAlias().hasNext());
        assertTrue(config.isIncludeAllFields());
        assertEquals(WriteModeEnum.INSERT, config.getWriteMode());
    }

    @Test
    public void handleTargetAndSourceContainingDash() {
        String topic = "TOPIC-A";
        String table = "TABLE-A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s", table, topic);
        Config config = Config.parse(syntax);
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
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
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
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
        assertEquals(Config.DEFAULT_BATCH_SIZE, config.getBatchSize());
    }

    @Test
    public void parseAnInsertWithFieldAliasAndSettingTheBatchSize() {
        String topic = "TOPIC-A";
        String table = "TABLE_A";
        String batchSize = "500";
        String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2 FROM %s BATCH = %s", table, topic, batchSize);
        Config config = Config.parse(syntax);
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
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
        assertEquals(500, config.getBatchSize());
    }

    @Test
    public void parseAnInsertWithFieldAliasMixedWithNoAliasing() {
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
        assertEquals(WriteModeEnum.INSERT, config.getWriteMode());
    }

    @Test
    public void parseAnInsertWithFieldAliasMixedWithAllFieldsTheAsterixAtTheEnd() {
        String topic = "TOPIC+A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT f1 as col1, * FROM %s", table, topic);
        Config config = Config.parse(syntax);
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
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
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
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
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
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
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
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
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
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
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
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
        assertFalse(config.isEnableCapitalize());
    }

    @Test
    public void parseWithInitialize() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s batch = 100 initialize", table, topic);
        Config config = Config.parse(syntax);
        assertTrue(config.isInitialize());
    }

    @Test
    public void parseWithWithOutInitialize() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT * FROM %s IGNORE col1, 1col2 ", table, topic);
        Config config = Config.parse(syntax);
        assertFalse(config.isInitialize());
    }

    @Test
    public void parseWithProject() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s batch = 100 initialize projectTo 1", table, topic);
        Config config = Config.parse(syntax);
        assertTrue(config.getProjectTo().equals(1));
    }


    @Test
    public void parseAnInsertWithFieldAliasAndAutocreateNoPKs() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2 FROM %s AUTOCREATE", table, topic);
        Config config = Config.parse(syntax);
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
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
        assertTrue(config.isAutoCreate());
        assertFalse(config.getPrimaryKeys().hasNext());
        assertEquals(WriteModeEnum.INSERT, config.getWriteMode());
    }

    @Test
    public void parseAnInsertWithFieldAliasAndAutocreateWithPKs() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2, col3 FROM %s AUTOCREATE PK col1,col3", table, topic);
        Config config = Config.parse(syntax);
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
        List<FieldAlias> fa = Lists.newArrayList(config.getFieldAlias());
        Map<String, FieldAlias> map = new HashMap<>();
        for (FieldAlias alias : fa) {
            map.put(alias.getField(), alias);
        }
        assertEquals(3, fa.size());
        assertTrue(map.containsKey("f1"));
        assertEquals("col1", map.get("f1").getAlias());
        assertTrue(map.containsKey("f2"));
        assertEquals("col2", map.get("f2").getAlias());
        assertTrue(map.containsKey("col3"));
        assertEquals("col3", map.get("col3").getAlias());
        assertFalse(config.isIncludeAllFields());
        assertTrue(config.isAutoCreate());

        Iterator<String> pksIterator = config.getPrimaryKeys();
        HashSet<String> pks = new HashSet<>();
        while (pksIterator.hasNext()) {
            pks.add(pksIterator.next());
        }

        assertEquals(2, pks.size());
        assertTrue(pks.contains("col1"));
        assertTrue(pks.contains("col3"));
        assertEquals(WriteModeEnum.INSERT, config.getWriteMode());

        assertFalse(config.isAutoEvolve());
    }

    @Test
    public void parseAnInsertWithFieldAliasAndAutocreateWithPKsAndAutoevolve() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2, col3 FROM %s AUTOCREATE PK col1,col3 AUTOEVOLVE", table, topic);
        Config config = Config.parse(syntax);
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
        List<FieldAlias> fa = Lists.newArrayList(config.getFieldAlias());
        Map<String, FieldAlias> map = new HashMap<>();
        for (FieldAlias alias : fa) {
            map.put(alias.getField(), alias);
        }
        assertEquals(3, fa.size());
        assertTrue(map.containsKey("f1"));
        assertEquals("col1", map.get("f1").getAlias());
        assertTrue(map.containsKey("f2"));
        assertEquals("col2", map.get("f2").getAlias());
        assertTrue(map.containsKey("col3"));
        assertEquals("col3", map.get("col3").getAlias());
        assertFalse(config.isIncludeAllFields());
        assertTrue(config.isAutoCreate());

        Iterator<String> pksIterator = config.getPrimaryKeys();
        HashSet<String> pks = new HashSet<>();
        while (pksIterator.hasNext()) {
            pks.add(pksIterator.next());
        }

        assertEquals(2, pks.size());
        assertTrue(pks.contains("col1"));
        assertTrue(pks.contains("col3"));
        assertEquals(WriteModeEnum.INSERT, config.getWriteMode());

        assertTrue(config.isAutoEvolve());
    }

  /*
  // Those rules are valid for RDBMS KCQL - but we relax to support other target systems
  @Test(expected = IllegalArgumentException.class)
  public void throwsErrorWhenThePKIsNotPresentInTheSelectClause() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2, col3 FROM %s AUTOCREATE PK col1,colX", table, topic);
    Config.parse(syntax);
  }


  @Test(expected = IllegalArgumentException.class)
  public void throwsErrorWhenThePKIsNotPresentInTheSelectClauseSinglePK() {
    String syntax = "INSERT INTO someTable SELECT lastName as surname, firstName FROM someTable PK IamABadPersonAndIHateYou";
    Config.parse(syntax);
  }
  */

    @Test
    public void parseAnUpsertWithSelectAllFieldsWithIgnoredColumnsWithCapitalization() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT * FROM %s IGNORE col1, 1col2 CAPITALIZE  ", table, topic);
        Config config = Config.parse(syntax);
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
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
        assertTrue(config.isEnableCapitalize());
    }

    @Test
    public void handlerPartitionByWhenAllFieldsAreIncluded() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT * FROM %s IGNORE col1, 1col2 PARTITIONBY col1,col2  ", table, topic);
        Config config = Config.parse(syntax);

        Set<String> partitionBy = new HashSet<>();
        Iterator<String> iter = config.getPartitionBy();
        while (iter.hasNext()) {
            partitionBy.add(iter.next());
        }

        assertTrue(partitionBy.contains("col1"));
        assertTrue(partitionBy.contains("col2"));
    }

    @Test
    public void handlerPartitionByWhenSpecificFieldsAreIncluded() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT col1, col2, col3 FROM %s IGNORE col1, 1col2 PARTITIONBY col1,col2  ", table, topic);
        Config config = Config.parse(syntax);

        Set<String> partitionBy = new HashSet<>();
        Iterator<String> iter = config.getPartitionBy();
        while (iter.hasNext()) {
            partitionBy.add(iter.next());
        }

        assertTrue(partitionBy.contains("col1"));
        assertTrue(partitionBy.contains("col2"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsAnExceptionIfThePartitionByFieldIsNotPresentInTheListOfField() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT col1, col3 FROM %s IGNORE col1, 1col2 PARTITIONBY col1,col2  ", table, topic);
        Config.parse(syntax);
    }

    @Test
    public void handlerPartitionByWhenSpecificFieldsAreIncludedAndAliasingIsPresent() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT col1, col2 as colABC, col3 FROM %s IGNORE col1, 1col2 PARTITIONBY col1,colABC ", table, topic);
        Config config = Config.parse(syntax);

        Set<String> partitionBy = new HashSet<>();
        Iterator<String> iter = config.getPartitionBy();
        while (iter.hasNext()) {
            partitionBy.add(iter.next());
        }

        assertTrue(partitionBy.contains("col1"));
        assertTrue(partitionBy.contains("colABC"));
    }

    @Test
    public void handlerDistributeWhenAllFieldsAreIncluded() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT * FROM %s IGNORE col1, 1col2 DISTRIBUTEBY col1,col2 INTO 10 BUCKETS", table, topic);
        Config config = Config.parse(syntax);

        Bucketing bucketing = config.getBucketing();
        assertNotNull(bucketing);
        HashSet<String> bucketNames = new HashSet<>();
        Iterator<String> iter = bucketing.getBucketNames();
        while (iter.hasNext()) {
            bucketNames.add(iter.next());
        }
        assertEquals(2, bucketNames.size());
        assertTrue(bucketNames.contains("col2"));
        assertEquals(10, bucketing.getBucketsNumber());
    }

    @Test
    public void handlerDistributeWhenSpecificFieldsAreIncluded() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT col1, col2, col3 FROM %s IGNORE col1, 1col2 DISTRIBUTEBY col1,col2 INTO 10 BUCKETS", table, topic);
        Config config = Config.parse(syntax);


        Bucketing bucketing = config.getBucketing();
        assertNotNull(bucketing);
        HashSet<String> bucketNames = new HashSet<>();
        Iterator<String> iter = bucketing.getBucketNames();
        while (iter.hasNext()) {
            bucketNames.add(iter.next());
        }
        assertEquals(2, bucketNames.size());
        assertTrue(bucketNames.contains("col2"));
        assertEquals(10, bucketing.getBucketsNumber());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsAnExceptionIfTheDistributeByFieldIsNotPresentInTheListOfField() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT col1, col3 FROM %s IGNORE col1, 1col2 DISTRIBUTEBY col1,col2 INTO 10 BUCKETS ", table, topic);
        Config.parse(syntax);
    }

    @Test
    public void handlerDistributeByWhenSpecificFieldsAreIncludedAndAliasingIsPresent() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT col1, col2 as colABC, col3 FROM %s IGNORE col1, 1col2 DISTRIBUTEBY col1,colABC INTO 10 BUCKETS ", table, topic);
        Config config = Config.parse(syntax);

        Bucketing bucketing = config.getBucketing();
        assertNotNull(bucketing);
        HashSet<String> bucketNames = new HashSet<>();
        Iterator<String> iter = bucketing.getBucketNames();
        while (iter.hasNext()) {
            bucketNames.add(iter.next());
        }
        assertEquals(2, bucketNames.size());
        assertTrue(bucketNames.contains("colABC"));
        assertEquals(10, bucketing.getBucketsNumber());
    }

    @Test
    public void handlerBucketingWithAllColumnsSelected() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT * FROM %s PARTITIONBY col1,colABC CLUSTERBY col2 INTO 256 BUCKETS", table, topic);
        Config config = Config.parse(syntax);

        Bucketing bucketing = config.getBucketing();
        assertNotNull(bucketing);
        HashSet<String> bucketNames = new HashSet<>();
        Iterator<String> iter = bucketing.getBucketNames();
        while (iter.hasNext()) {
            bucketNames.add(iter.next());
        }
        assertEquals(1, bucketNames.size());
        assertTrue(bucketNames.contains("col2"));
        assertEquals(256, bucketing.getBucketsNumber());
    }

    @Test
    public void handlerBucketingWithSpecificColumnsSpecified() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s CLUSTERBY col2 INTO 256 BUCKETS", table, topic);
        Config config = Config.parse(syntax);

        Bucketing bucketing = config.getBucketing();
        assertNotNull(bucketing);
        HashSet<String> bucketNames = new HashSet<>();
        Iterator<String> iter = bucketing.getBucketNames();
        while (iter.hasNext()) {
            bucketNames.add(iter.next());
        }
        assertEquals(1, bucketNames.size());
        assertTrue(bucketNames.contains("col2"));
        assertEquals(256, bucketing.getBucketsNumber());
    }

    @Test
    public void handleDashForTopicAndTable() {
        String topic = "TOPIC-A-A";
        String table = "TABLE-A";
        String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s CLUSTERBY col2 INTO 256 BUCKETS", table, topic);
        Config config = Config.parse(syntax);

        Bucketing bucketing = config.getBucketing();
        assertNotNull(bucketing);
        HashSet<String> bucketNames = new HashSet<>();
        Iterator<String> iter = bucketing.getBucketNames();
        while (iter.hasNext()) {
            bucketNames.add(iter.next());
        }
        assertEquals(1, bucketNames.size());
        assertTrue(bucketNames.contains("col2"));
        assertEquals(256, bucketing.getBucketsNumber());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwExceptionIfTheBucketsIsZero() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s CLUSTERBY col2 INTO 0 BUCKETS", table, topic);
        Config.parse(syntax);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwExceptionIfTheBucketsNumberIsNotProvided() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s CLUSTERBY col2", table, topic);
        Config.parse(syntax);
    }


    @Test(expected = IllegalArgumentException.class)
    public void throwExceptionIfTheBucketNamesAreMissing() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s CLUSTERBY  INTO 12 BUCKETS", table, topic);
        Config.parse(syntax);
    }

    @Test
    public void handleTimestampAsOneOfTheFields() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s WITHTIMESTAMP col1", table, topic);
        Config c = Config.parse(syntax);
        assertEquals(c.getTimestamp(), "col1");
    }

    @Test
    public void handleTimestampWhenAllFieldIncluded() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s WITHTIMESTAMP col1", table, topic);
        Config c = Config.parse(syntax);
        assertEquals(c.getTimestamp(), "col1");
    }

    @Test
    public void handleTimestampSetAsCurrentSysWhenAllFieldsIncluded() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s WITHTIMESTAMP " + Config.TIMESTAMP, table, topic);
        Config c = Config.parse(syntax);
        assertEquals(c.getTimestamp(), Config.TIMESTAMP);
    }

    @Test
    public void handleFieldSelectionWithPKWithTimestampSetAsFieldNotInSelection() {
        String syntax = "INSERT INTO measurements SELECT actualTemperature, targetTemperature FROM TOPIC_A PK machineId, type WITHTIMESTAMP ts";
        Config c = Config.parse(syntax);
        assertEquals(c.getTimestamp(), "ts");

        HashSet<String> pks = new HashSet<>();
        Iterator<String> iter = c.getPrimaryKeys();
        while (iter.hasNext()) {
            pks.add(iter.next());
        }
        assertEquals(2, pks.size());
        assertTrue(pks.contains("type"));
        assertTrue(pks.contains("machineId"));
    }

    @Test
    public void handleTimestampSetAsCurrentSysWhenSelectedFieldsIncluded() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT col1, col2,col3 FROM %s WITHTIMESTAMP " + Config.TIMESTAMP, table, topic);
        Config c = Config.parse(syntax);
        assertEquals(c.getTimestamp(), Config.TIMESTAMP);
    }

    @Test
    public void handleStoredAs() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s WITHFORMAT avro", table, topic);
        Config c = Config.parse(syntax);
        assertEquals(c.getFormatType().toString(), "AVRO");

        String syntax2 = String.format("INSERT INTO %s SELECT * FROM %s WITHFORMAT json", table, topic);
        Config c2 = Config.parse(syntax2);
        assertEquals(c2.getFormatType().toString(), "JSON");

        String syntax3 = String.format("INSERT INTO %s SELECT * FROM %s WITHFORMAT map", table, topic);
        Config c3 = Config.parse(syntax3);
        assertEquals(c3.getFormatType().toString(), "MAP");

        String syntax4 = String.format("INSERT INTO %s SELECT * FROM %s WITHFORMAT object", table, topic);
        Config c4 = Config.parse(syntax4);
        assertEquals(c4.getFormatType().toString(), "OBJECT");
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwExceptionIfStoredAsTypeIsMissing() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s STOREAS", table, topic);
        Config.parse(syntax);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfStoredAsParametersIsEmpty() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s STOREAS SS ()", table, topic);
        Config config = Config.parse(syntax);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfStoredAsParameterAppersTwice() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s STOREAS SS (name = something , NaMe= something)", table, topic);
        Config.parse(syntax);
    }

    @Test
    public void handleStoredAsClause() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s STOREAS SS (param1 = value1 , param2 = value2,param3=value3)", table, topic);
        Config config = Config.parse(syntax);
        assertEquals("SS", config.getStoredAs());
        assertEquals(3, config.getStoredAsParameters().size());
        assertEquals("value1", config.getStoredAsParameters().get("param1"));
        assertEquals("value2", config.getStoredAsParameters().get("param2"));
        assertEquals("value3", config.getStoredAsParameters().get("param3"));
    }

    @Test
    public void handleTTLClause() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s STOREAS SS (param1 = value1 , param2 = value2,param3=value3) TTL=999", table, topic);
        Config config = Config.parse(syntax);
        assertEquals("SS", config.getStoredAs());
        assertEquals(3, config.getStoredAsParameters().size());
        assertEquals("value1", config.getStoredAsParameters().get("param1"));
        assertEquals("value2", config.getStoredAsParameters().get("param2"));
        assertEquals("value3", config.getStoredAsParameters().get("param3"));
        assertEquals(999, config.getTTL());
    }


    @Test
    public void handleForwardSlashInSource() {
        String topic = "/TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s", table, topic);
        Config config = Config.parse(syntax);
        assertEquals(topic, config.getSource());
    }

    @Test
    public void handleWithUnwrap() {
        String topic = "/TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s withunwrap", table, topic);
        Config config = Config.parse(syntax);
        assertEquals(topic, config.getSource());
        assertTrue(config.isWithUnwrap());
    }

    @Test
    public void parseTags() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("INSERT INTO %s SELECT * FROM %s WITHTAG (field1, c1=v1, field2, c2=v2)", table, topic);
        Config config = Config.parse(syntax);
        assertEquals(topic, config.getSource());
        assertEquals(table, config.getTarget());
        assertFalse(config.getFieldAlias().hasNext());
        assertTrue(config.isIncludeAllFields());
        assertEquals(WriteModeEnum.INSERT, config.getWriteMode());

        Map<String, Tag> tagsMap = new HashMap<>();
        Iterator<Tag> iterTags = config.getTags();
        while (iterTags.hasNext()) {
            Tag tag = iterTags.next();
            tagsMap.put(tag.getKey(), tag);
        }

        assertEquals(4, tagsMap.size());
        assertTrue(tagsMap.containsKey("field1"));
        assertFalse(tagsMap.get("field1").isConstant());
        assertTrue(tagsMap.containsKey("field2"));
        assertFalse(tagsMap.get("field2").isConstant());

        assertTrue(tagsMap.containsKey("c2"));
        assertTrue(tagsMap.get("c2").isConstant());

        assertTrue(tagsMap.containsKey("c1"));
        assertTrue(tagsMap.get("c1").isConstant());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTagsListIsEmpty() {
        String topic = "TOPIC_A";
        String table = "TABLE_A";
        String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s WITHTAGS ()", table, topic);
        Config.parse(syntax);
    }

}
