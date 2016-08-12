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
import static org.junit.Assert.assertTrue;

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

  @Test(expected = IllegalArgumentException.class)
  public void throwExceptionIfTheTimestampIsNotInTheSelectedFields() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s WITHTIMESTAMP notpresent", table, topic);
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
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s STOREDAS x ", table, topic);
    Config c = Config.parse(syntax);
    assertEquals(c.getStoredAs(), "x");

    String syntax2 = String.format("INSERT INTO %s SELECT * FROM %s storedas x ", table, topic);
    Config c2 = Config.parse(syntax2);
    assertEquals(c2.getStoredAs(), "x");
  }
}
