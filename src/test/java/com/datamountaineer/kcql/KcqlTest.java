package com.datamountaineer.kcql;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class KcqlTest {

  @Test
  public void parseAnInsertWithSelectAllFieldsAndNoIgnoreAndPKs() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s PK f1,f2", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertTrue(kcql.getFields().get(0).getName().equals("*"));
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
    HashSet<String> pks = new HashSet<>();
    kcql.getPrimaryKeys().forEach(f -> pks.add(f.toString()));

    assertEquals(2, pks.size());
    assertTrue(pks.contains("f1"));
    assertTrue(pks.contains("f2"));
    assertNull(kcql.getTags());
    assertFalse(kcql.isUnwrapping());
  }

  @Test
  public void parseSimpleSelectCommand() {
    String syntax = "SELECT * FROM topicA";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("topicA", kcql.getSource());
  }

  @Test
  public void parseSimpleSelectCommandWithPK() {
    String syntax = "SELECT * FROM topicA PK lastName";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("topicA", kcql.getSource());
  }

  @Test
  public void parseAnotherSimpleSelectCommandWithPK() {
    String syntax = "SELECT firstName, lastName as surname FROM topicA";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("topicA", kcql.getSource());
  }

  @Test
  public void parseAnInsertWithSelectAllFieldsAndNoIgnore() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertTrue(kcql.getFields().get(0).getName().equals("*"));
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  public void handleTargetAndSourceContainingDot() {
    String topic = "TOPIC.A";
    String table = "TABLE.A";
    String syntax = String.format("INSERT INTO `%s` SELECT * FROM `%s`", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertTrue(kcql.getFields().get(0).getName().equals("*"));
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  public void handleTargetAndSourceContainingDash() {
    String topic = "TOPIC-A";
    String table = "TABLE-A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertTrue(kcql.getFields().get(0).getName().equals("*"));

    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  public void parseAnInsertWithFieldAlias() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2 FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
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
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  public void parseAnInsertWithFieldAliasAndSettingTheBatchSize() {
    String topic = "TOPIC-A";
    String table = "TABLE_A";
    String batchSize = "500";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2 FROM %s BATCH = %s", table, topic, batchSize);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
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
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
    assertEquals(500, kcql.getBatchSize());
  }

  @Test
  public void parseAnInsertWithFieldAliasMixedWithNoAliasing() {
    String topic = "TOPIC.A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f3, f2 as col2,f4 FROM `%s`", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
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
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  public void parseAnInsertWithFieldAliasMixedWithAllFieldsTheAsterixAtTheEnd() {
    String topic = "TOPIC+A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, * FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    List<Field> fa = Lists.newArrayList(kcql.getFields());
    Map<String, Field> map = new HashMap<>();
    for (Field alias : fa) {
      map.put(alias.getName(), alias);
    }
    assertEquals(2, fa.size());
    assertTrue(map.containsKey("f1"));
    assertTrue(map.containsKey("*"));
    assertEquals("col1", map.get("f1").getAlias());
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  public void parseAnInsertWithDottedTarget() {
    String topic = "TOPIC+A";
    String table = "KEYSPACE.A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, * FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
  }

  @Test
  public void parseAnInsertWithFieldAliasMixedWithAllFieldsTheAsterixAtTheBegining() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT *,f1 as col1 FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    List<Field> fa = Lists.newArrayList(kcql.getFields());
    Map<String, Field> map = new HashMap<>();
    for (Field alias : fa) {
      map.put(alias.getName(), alias);
    }
    assertEquals(2, fa.size());
    assertTrue(map.containsKey("f1"));
    assertTrue(map.containsKey("*"));
    assertEquals("col1", map.get("f1").getAlias());
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  public void parseAnInsertWithFieldAliasMixedWithAllFieldsTheAsterixInTheMiddle() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f2 as col2,*,f1 as col1 FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    List<Field> fa = Lists.newArrayList(kcql.getFields());
    Map<String, Field> map = new HashMap<>();
    for (Field alias : fa) {
      map.put(alias.getName(), alias);
    }
    assertEquals(3, fa.size());
    assertTrue(map.containsKey("*"));
    assertTrue(map.containsKey("f1"));
    assertEquals("col1", map.get("f1").getAlias());
    assertTrue(map.containsKey("f2"));
    assertEquals("col2", map.get("f2").getAlias());
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }


  @Test
  public void parseAnUpsertWithSelectAllFieldsAndNoIgnore() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT * FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertTrue(kcql.getFields().get(0).getName().equals("*"));
    assertEquals(WriteModeEnum.UPSERT, kcql.getWriteMode());
  }

  @Test
  public void parseAnInsertWithSelectAllFieldsWithIgnoredColumns() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s IGNORE col1 , col2 ", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertTrue(kcql.getFields().get(0).getName().equals("*"));
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
    List<Field> ignored = kcql.getIgnoredFields();

    assertEquals(ignored.get(0).getName(), "col1");
    assertEquals(ignored.get(1).getName(), "col2");

  }

  @Test
  public void parseAnUpsertWithSelectAllFieldsWithIgnoredColumns() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT * FROM %s IGNORE col1, 1col2  ", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertTrue(kcql.getFields().get(0).getName().equals("*"));

    assertEquals(WriteModeEnum.UPSERT, kcql.getWriteMode());
    List<Field> ignored = kcql.getIgnoredFields();

    assertEquals(ignored.get(0).getName(), "col1");
    assertEquals(ignored.get(1).getName(), "1col2");
    assertFalse(kcql.isEnableCapitalize());
  }

  @Test
  public void parseWithInitialize() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s batch = 100 initialize", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertTrue(kcql.isInitialize());
  }

  @Test
  public void parseWithWithOutInitialize() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT * FROM %s IGNORE col1, 1col2 ", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertFalse(kcql.isInitialize());
  }

  @Test
  public void parseWithProject() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s batch = 100 initialize projectTo 1", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertTrue(kcql.getProjectTo().equals(1));
  }


  @Test
  public void parseAnInsertWithFieldAliasAndAutocreateNoPKs() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2 FROM %s AUTOCREATE", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
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
    assertTrue(kcql.isAutoCreate());
    assertTrue(kcql.getPrimaryKeys().isEmpty());
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());
  }

  @Test
  public void parseAnInsertWithFieldAliasAndAutocreateWithPKs() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2, col3 FROM %s AUTOCREATE PK col1,col3", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    List<Field> fa = Lists.newArrayList(kcql.getFields());
    Map<String, Field> map = new HashMap<>();
    for (Field alias : fa) {
      map.put(alias.getName(), alias);
    }
    assertEquals(3, fa.size());
    assertTrue(map.containsKey("f1"));
    assertEquals("col1", map.get("f1").getAlias());
    assertTrue(map.containsKey("f2"));
    assertEquals("col2", map.get("f2").getAlias());
    assertTrue(map.containsKey("col3"));
    assertEquals("col3", map.get("col3").getAlias());
    assertTrue(kcql.isAutoCreate());

    HashSet<String> pks = new HashSet<>();
    kcql.getPrimaryKeys().forEach(f -> pks.add(f.toString()));

    assertEquals(2, pks.size());
    assertTrue(pks.contains("col1"));
    assertTrue(pks.contains("col3"));
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());

    assertFalse(kcql.isAutoEvolve());
  }

  @Test
  public void parseAnInsertWithFieldAliasAndAutocreateWithPKsAndAutoevolve() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2, col3 FROM %s AUTOCREATE PK col1,col3 AUTOEVOLVE", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    List<Field> fa = Lists.newArrayList(kcql.getFields());
    Map<String, Field> map = new HashMap<>();
    for (Field alias : fa) {
      map.put(alias.getName(), alias);
    }
    assertEquals(3, fa.size());
    assertTrue(map.containsKey("f1"));
    assertEquals("col1", map.get("f1").getAlias());
    assertTrue(map.containsKey("f2"));
    assertEquals("col2", map.get("f2").getAlias());
    assertTrue(map.containsKey("col3"));
    assertEquals("col3", map.get("col3").getAlias());

    assertTrue(kcql.isAutoCreate());

    HashSet<String> pks = new HashSet<>();
    kcql.getPrimaryKeys().forEach(f -> pks.add(f.toString()));


    assertEquals(2, pks.size());
    assertTrue(pks.contains("col1"));
    assertTrue(pks.contains("col3"));
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());

    assertTrue(kcql.isAutoEvolve());
  }

  /*
  // Those rules are valid for RDBMS KCQL - but we relax to support other target systems
  @Test(expected = IllegalArgumentException.class)
  public void throwsErrorWhenThePKIsNotPresentInTheSelectClause() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT f1 as col1, f2 as col2, col3 FROM %s AUTOCREATE PK col1,colX", table, topic);
    Kcql.parse(syntax);
  }


  @Test(expected = IllegalArgumentException.class)
  public void throwsErrorWhenThePKIsNotPresentInTheSelectClauseSinglePK() {
    String syntax = "INSERT INTO someTable SELECT lastName as surname, firstName FROM someTable PK IamABadPersonAndIHateYou";
    Kcql.parse(syntax);
  }
  */

  @Test
  public void parseAnUpsertWithSelectAllFieldsWithIgnoredColumnsWithCapitalization() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT * FROM %s IGNORE col1, 1col2 CAPITALIZE  ", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertTrue(kcql.getFields().get(0).getName().equals("*"));

    assertEquals(WriteModeEnum.UPSERT, kcql.getWriteMode());
    List<Field> ignored = kcql.getIgnoredFields();

    assertEquals(ignored.get(0).getName(), "col1");
    assertEquals(ignored.get(1).getName(), "1col2");
    assertTrue(kcql.isEnableCapitalize());
  }

  @Test
  public void handlerPartitionByWhenAllFieldsAreIncluded() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT * FROM %s IGNORE col1, 1col2 PARTITIONBY col1,col2  ", table, topic);
    Kcql kcql = Kcql.parse(syntax);

    Set<String> partitionBy = new HashSet<>();
    Iterator<String> iter = kcql.getPartitionBy();
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
    Kcql kcql = Kcql.parse(syntax);

    Set<String> partitionBy = new HashSet<>();
    Iterator<String> iter = kcql.getPartitionBy();
    while (iter.hasNext()) {
      partitionBy.add(iter.next());
    }

    assertTrue(partitionBy.contains("col1"));
    assertTrue(partitionBy.contains("col2"));
  }

  @Test
  public void handlerPartitionByWhenSpecificFieldsAreIncludedAndAliasingIsPresent() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT col1, col2 as colABC, col3 FROM %s IGNORE col1, 1col2 PARTITIONBY col1,colABC ", table, topic);
    Kcql kcql = Kcql.parse(syntax);

    Set<String> partitionBy = new HashSet<>();
    Iterator<String> iter = kcql.getPartitionBy();
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
    Kcql kcql = Kcql.parse(syntax);

    Bucketing bucketing = kcql.getBucketing();
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
    Kcql kcql = Kcql.parse(syntax);


    Bucketing bucketing = kcql.getBucketing();
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
  public void handlerDistributeByWhenSpecificFieldsAreIncludedAndAliasingIsPresent() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT col1, col2 as colABC, col3 FROM %s IGNORE col1, 1col2 DISTRIBUTEBY col1,colABC INTO 10 BUCKETS ", table, topic);
    Kcql kcql = Kcql.parse(syntax);

    Bucketing bucketing = kcql.getBucketing();
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
    Kcql kcql = Kcql.parse(syntax);

    Bucketing bucketing = kcql.getBucketing();
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
    Kcql kcql = Kcql.parse(syntax);

    Bucketing bucketing = kcql.getBucketing();
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
    Kcql kcql = Kcql.parse(syntax);

    Bucketing bucketing = kcql.getBucketing();
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
    Kcql.parse(syntax);
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwExceptionIfTheBucketsNumberIsNotProvided() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s CLUSTERBY col2", table, topic);
    Kcql.parse(syntax);
  }


  @Test(expected = IllegalArgumentException.class)
  public void throwExceptionIfTheBucketNamesAreMissing() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s CLUSTERBY  INTO 12 BUCKETS", table, topic);
    Kcql.parse(syntax);
  }

  @Test
  public void handleTimestampAsOneOfTheFields() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s WITHTIMESTAMP col1", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(kcql.getTimestamp(), "col1");
  }

  @Test
  public void handleTypeAsOneOfTheFields() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s WITHTYPE QUEUE", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("QUEUE", kcql.getWithType());
  }

  @Test
  public void handleCompoundWITHFields() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s WITHTYPE QUEUE WITHCONVERTER=`com.blah.Converter` WITHJMSSELECTOR=`apples > 10`", table, topic);

    Kcql kcql = Kcql.parse(syntax);
    assertEquals("QUEUE", kcql.getWithType());
    assertEquals("com.blah.Converter", kcql.getWithConverter());
    assertEquals("apples > 10", kcql.getWithJmsSelector());
  }

  @Test
  public void handleTimestampWhenAllFieldIncluded() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s WITHTIMESTAMP col1", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(kcql.getTimestamp(), "col1");
  }

  @Test
  public void handleTimestampSetAsCurrentSysWhenAllFieldsIncluded() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s WITHTIMESTAMP " + Kcql.TIMESTAMP, table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(kcql.getTimestamp(), Kcql.TIMESTAMP);
  }

  @Test
  public void handleFieldSelectionWithPKWithTimestampSetAsFieldNotInSelection() {
    String syntax = "INSERT INTO measurements SELECT actualTemperature, targetTemperature FROM TOPIC_A PK machineId, type WITHTIMESTAMP ts";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(kcql.getTimestamp(), "ts");

    HashSet<String> pks = new HashSet<>();
    kcql.getPrimaryKeys().forEach(f -> pks.add(f.toString()));


    assertEquals(2, pks.size());
    assertTrue(pks.contains("type"));
    assertTrue(pks.contains("machineId"));
  }

  @Test
  public void handleTimestampSetAsCurrentSysWhenSelectedFieldsIncluded() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1, col2,col3 FROM %s WITHTIMESTAMP " + Kcql.TIMESTAMP, table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(kcql.getTimestamp(), Kcql.TIMESTAMP);
  }

  @Test
  public void handleAtCharacterInFields() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT @col1, col2,col3 FROM %s WITHTIMESTAMP " + Kcql.TIMESTAMP, table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(kcql.getFields().get(0).getName(), "@col1");
  }

  @Test
  public void handleKeyDelimeter() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = "INSERT INTO %s SELECT @col1, col2,col3 FROM %s KEYDELIMITER ='|'";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("|", kcql.getKeyDelimeter());
  }

  @Test
  public void handleWithKey() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = "INSERT INTO %s SELECT @col1, col2,col3 FROM %s WITHKEY(col1, col2, col3)";
    Kcql kcql = Kcql.parse(syntax);
    List<String> withKeys = kcql.getWithKeys();
    assertEquals("col1", withKeys.get(0));
    assertEquals("col2", withKeys.get(1));
    assertEquals("col3", withKeys.get(2));
    assertEquals(3, withKeys.size());
  }

  @Test
  public void handleStoredAs() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s WITHFORMAT avro", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(kcql.getFormatType().toString(), "AVRO");

    String syntax2 = String.format("INSERT INTO %s SELECT * FROM %s WITHFORMAT json", table, topic);
    Kcql c2 = Kcql.parse(syntax2);
    assertEquals(c2.getFormatType().toString(), "JSON");

    String syntax3 = String.format("INSERT INTO %s SELECT * FROM %s WITHFORMAT map", table, topic);
    Kcql c3 = Kcql.parse(syntax3);
    assertEquals(c3.getFormatType().toString(), "MAP");

    String syntax4 = String.format("INSERT INTO %s SELECT * FROM %s WITHFORMAT object", table, topic);
    Kcql c4 = Kcql.parse(syntax4);
    assertEquals(c4.getFormatType().toString(), "OBJECT");
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwExceptionIfStoredAsTypeIsMissing() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s STOREAS", table, topic);
    Kcql.parse(syntax);
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfStoredAsParametersIsEmpty() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s STOREAS SS ()", table, topic);
    Kcql kcql = Kcql.parse(syntax);
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfStoredAsParameterAppersTwice() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s STOREAS SS (name = something , NaMe= something)", table, topic);
    Kcql.parse(syntax);
  }

  @Test
  public void handleStoredAsClause() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s STOREAS SS (param1 = value1 , param2 = value2,param3=value3)", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("SS", kcql.getStoredAs());
    assertEquals(3, kcql.getStoredAsParameters().size());
    assertEquals("value1", kcql.getStoredAsParameters().get("param1"));
    assertEquals("value2", kcql.getStoredAsParameters().get("param2"));
    assertEquals("value3", kcql.getStoredAsParameters().get("param3"));
  }

  @Test
  public void handleSemicolonInTarget() {
    String topic = "TOPIC_A";
    String table = "namespace1:TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
  }

  @Test
  public void handleForwardSlashInSource() {
    String topic = "/TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
  }

  @Test
  public void handleTimestampUnit() {
    String topic = "/TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s TIMESTAMPUNIT=SECONDS", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(TimeUnit.SECONDS, kcql.getTimestampUnit());
  }


  @Test
  public void handleWithTarget() {
    String topic = "/TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s WITHTARGET = field1.field2.field3 WITHFORMAT object", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("field1.field2.field3", kcql.getDynamicTarget());
  }

  @Test
  public void parseTags() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s WITHTAG (field1, c1=v1, field2, c2=v2, field1.field2 as namedTag)", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertTrue(kcql.getFields().get(0).getName().equals("*"));
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());

    Map<String, Tag> tagsMap = new HashMap<>();
    Iterator<Tag> iterTags = kcql.getTags().iterator();
    while (iterTags.hasNext()) {
      Tag tag = iterTags.next();
      tagsMap.put(tag.getKey(), tag);
    }

    assertEquals(5, tagsMap.size());
    assertTrue(tagsMap.containsKey("field1"));
    assertEquals(Tag.TagType.DEFAULT, tagsMap.get("field1").getType());
    assertTrue(tagsMap.containsKey("field2"));
    assertEquals(Tag.TagType.DEFAULT, tagsMap.get("field2").getType());

    assertTrue(tagsMap.containsKey("c2"));
    assertEquals(Tag.TagType.CONSTANT, tagsMap.get("c2").getType());

    assertTrue(tagsMap.containsKey("c1"));
    assertEquals(Tag.TagType.CONSTANT, tagsMap.get("c1").getType());

    assertTrue(tagsMap.containsKey("field1.field2"));
    assertEquals(Tag.TagType.ALIAS, tagsMap.get("field1.field2").getType());
    assertEquals("namedTag", tagsMap.get("field1.field2").getValue());
  }

  @Test
  public void parseTagsWithNestedFields() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s WITHTAG (field1.fieldA, c1=v1, field2, c2=v2)", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(topic, kcql.getSource());
    assertEquals(table, kcql.getTarget());
    assertFalse(kcql.getFields().isEmpty());
    assertTrue(kcql.getFields().get(0).getName().equals("*"));
    assertEquals(WriteModeEnum.INSERT, kcql.getWriteMode());

    Map<String, Tag> tagsMap = new HashMap<>();
    Iterator<Tag> iterTags = kcql.getTags().iterator();
    while (iterTags.hasNext()) {
      Tag tag = iterTags.next();
      tagsMap.put(tag.getKey(), tag);
    }

    assertEquals(4, tagsMap.size());
    assertTrue(tagsMap.containsKey("field1.fieldA"));
    assertEquals(Tag.TagType.DEFAULT, tagsMap.get("field1.fieldA").getType());
    assertTrue(tagsMap.containsKey("field2"));
    assertEquals(Tag.TagType.DEFAULT, tagsMap.get("field2").getType());

    assertTrue(tagsMap.containsKey("c2"));
    assertEquals(Tag.TagType.CONSTANT, tagsMap.get("c2").getType());

    assertTrue(tagsMap.containsKey("c1"));
    assertEquals(Tag.TagType.CONSTANT, tagsMap.get("c1").getType());
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwExceptionWhenTagsWithNestedFieldsEndsWithDot() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT * FROM %s WITHTAG (field1.fieldA., c1=v1, field2, c2=v2)", table, topic);

    Kcql.parse(syntax);
  }


  @Test
  public void throwAnExceptionIfTagsListIsEmpty() {
    String topic = "TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("UPSERT INTO %s SELECT col1,col2 FROM %s WITHTAGS ()", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertNull(kcql.getTags());
  }

  @Test
  public void handleWithPipeline() {
    String topic = "/TOPIC_A";
    String table = "TABLE_A";
    String syntax = String.format("INSERT INTO %s SELECT col1,col2 FROM %s WITHPIPELINE = field1.field2.field3", table, topic);
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("field1.field2.field3", kcql.getPipeline());
  }

  @Test
  public void handleWithCompression() {

    String syntax = "INSERT INTO A SELECT * FROM B WITHPARTITIONER = SinglePartition WITHSUBSCRIPTION = shared WITHCOMPRESSION = SNAPPY WITHDELAY = 1000";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(CompressionType.SNAPPY, kcql.getWithCompression());
    assertEquals("SinglePartition", kcql.getWithPartitioner());
    assertEquals(1000, kcql.getWithDelay());
    assertEquals("shared", kcql.getWithSubscription());
  }

  @Test
  public void handleWithDelay() {
    String syntax = "INSERT INTO A SELECT * FROM B WITHDELAY = 1000";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals(Integer.parseInt("1000"), kcql.getWithDelay());
  }

  @Test
  public void handleWithSubscription() {
    String syntax = "INSERT INTO A SELECT * FROM B WITHSUBSCRIPTION = shared";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("shared", kcql.getWithSubscription());
  }

  @Test
  public void handleWithPartitioner() {
    String syntax = "INSERT INTO A SELECT * FROM B WITHPARTITIONER = shared";
    Kcql kcql = Kcql.parse(syntax);
    assertEquals("shared", kcql.getWithPartitioner());
  }

}
