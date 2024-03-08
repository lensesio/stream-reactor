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
package io.lenses.kcql

import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import org.scalatest.matchers.should.Matchers._

import java.util.concurrent.TimeUnit

class KcqlTest extends AnyFunSuite with OptionValues {
  test("parseAnInsertWithSelectAllFieldsAndNoIgnoreAndPKs") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT * FROM $topic PK f1,f2"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getFields should not be empty
    kcql.getFields.get(0).getName should be("*")
    kcql.getWriteMode should be(WriteModeEnum.INSERT)

    val pks = kcql.getPrimaryKeys.asScala.map(_.toString)
    pks should contain allOf ("f1", "f2")
    kcql.getTags should be(null)
    kcql.isUnwrapping should be(false)
  }

  test("parseSimpleSelectCommand") {
    val syntax = "SELECT * FROM topicA"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be("topicA")
  }
  test("parseSimpleSelectCommandWithPK") {
    val syntax = "SELECT * FROM topicA PK lastName"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be("topicA")
  }
  test("parseAnotherSimpleSelectCommandWithPK") {
    val syntax = "SELECT firstName, lastName as surname FROM topicA"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be("topicA")
    kcql.getFields.get(1).getName shouldBe "lastName"
    kcql.getFields.get(1).getAlias should be("surname")
  }

  test("parseAnInsertWithSelectAllFieldsAndNoIgnore") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT * FROM $topic"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getFields should not be empty
    kcql.getFields.get(0).getName should be("*")
    kcql.getWriteMode should be(WriteModeEnum.INSERT)
  }
  test("handleTargetAndSourceContainingDot") {
    val topic  = "TOPIC.A"
    val table  = "TABLE.A"
    val syntax = s"INSERT INTO `$table` SELECT * FROM `$topic`"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getFields should not be empty
    kcql.getFields.get(0).getName should be("*")
    kcql.getWriteMode should be(WriteModeEnum.INSERT)
  }
  test("handleTargetAndSourceContainingDash") {
    val topic  = "TOPIC-A"
    val table  = "TABLE-A"
    val syntax = s"INSERT INTO $table SELECT * FROM $topic"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getFields should not be empty
    kcql.getFields.get(0).getName should be("*")
    kcql.getWriteMode should be(WriteModeEnum.INSERT)
  }
  test("parseAnInsertWithFieldAlias") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT f1 as col1, f2 as col2 FROM $topic"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getWriteMode should be(WriteModeEnum.INSERT)

    val map = kcql.getFields.asScala.map(alias => alias.getName -> alias.getAlias).toMap
    map should have size 2
    map should contain allOf ("f1" -> "col1", "f2" -> "col2")
  }

  test("parseAnInsertWithFieldAliasAndSettingTheBatchSize") {
    val topic     = "TOPIC-A"
    val table     = "TABLE_A"
    val batchSize = "500"
    val syntax    = s"INSERT INTO $table SELECT f1 as col1, f2 as col2 FROM $topic BATCH = $batchSize"
    val kcql      = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getWriteMode should be(WriteModeEnum.INSERT)
    kcql.getBatchSize should be(500)

    val map = kcql.getFields.asScala.map(alias => alias.getName -> alias.getAlias).toMap
    map should have size 2
    map should contain allOf ("f1" -> "col1", "f2" -> "col2")
  }

  test("parseAnInsertWithFieldAliasMixedWithNoAliasing") {
    val topic  = "TOPIC.A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT f1 as col1, f3, f2 as col2,f4 FROM `$topic`"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getWriteMode should be(WriteModeEnum.INSERT)

    val map = kcql.getFields.asScala.map(alias => alias.getName -> alias.getAlias).toMap
    map should have size 4
    map should contain allOf (
      "f1" -> "col1",
      "f2" -> "col2",
      "f3" -> "f3",
      "f4" -> "f4"
    )
  }

  test("parseAnInsertWithFieldAliasMixedWithAllFieldsTheAsterixAtTheEnd") {
    val topic  = "TOPIC+A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT f1 as col1, * FROM $topic"

    val kcql = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getWriteMode should be(WriteModeEnum.INSERT)

    val map = kcql.getFields.asScala.map(alias => alias.getName -> alias.getAlias).toMap
    map should have size 2
    map should contain allOf (
      "f1" -> "col1",
      "*"  -> "*",
    )
  }

  test("parseAnInsertWithDottedTarget") {
    val topic  = "TOPIC+A"
    val table  = "KEYSPACE.A"
    val syntax = s"INSERT INTO $table SELECT f1 as col1, * FROM $topic"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
  }

  test("parseAnInsertWithFieldAliasMixedWithAllFieldsTheAsterixAtTheBegining") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT *,f1 as col1 FROM $topic"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getWriteMode should be(WriteModeEnum.INSERT)

    val map = kcql.getFields.asScala.map(alias => alias.getName -> alias.getAlias).toMap
    map should have size 2
    map should contain allOf (
      "f1" -> "col1",
      "*"  -> "*",
    )
  }

  test("parseAnInsertWithFieldAliasMixedWithAllFieldsTheAsterixInTheMiddle") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT f2 as col2,*,f1 as col1 FROM $topic"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getWriteMode should be(WriteModeEnum.INSERT)

    val map = kcql.getFields.asScala.map(alias => alias.getName -> alias.getAlias).toMap
    map should have size 3
    map should contain allOf (
      "f1" -> "col1",
      "f2" -> "col2",
      "*"  -> "*",
    )
  }

  test("parseAnUpsertWithSelectAllFieldsAndNoIgnore") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"UPSERT INTO $table SELECT * FROM $topic"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getFields should not be empty
    kcql.getFields.get(0).getName should be("*")
    kcql.getWriteMode should be(WriteModeEnum.UPSERT)
  }
  test("parseAnInsertWithSelectAllFieldsWithIgnoredColumns") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT * FROM $topic IGNORE col1 , col2 "
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getFields should not be empty
    kcql.getFields.get(0).getName should be("*")
    kcql.getWriteMode should be(WriteModeEnum.INSERT)
    kcql.getIgnoredFields.asScala.map(_.getName) should contain inOrder ("col1", "col2")
  }

  test("parseAnUpsertWithSelectAllFieldsWithIgnoredColumns") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"UPSERT INTO $table SELECT * FROM $topic IGNORE col1, 1col2  "
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getFields should not be empty
    kcql.getFields.get(0).getName should be("*")

    kcql.getWriteMode should be(WriteModeEnum.UPSERT)
    kcql.getIgnoredFields.asScala.map(_.getName) should contain inOrder ("col1", "1col2")
  }

  test("parseAnInsertWithFieldAliasAndAutocreateNoPKs") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT f1 as col1, f2 as col2 FROM $topic AUTOCREATE"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.isAutoCreate should be(true)
    kcql.getPrimaryKeys should be(empty)
    kcql.getWriteMode should be(WriteModeEnum.INSERT)

    val map = kcql.getFields.asScala.map(alias => alias.getName -> alias.getAlias).toMap
    map should have size 2
    map should contain allOf (
      "f1" -> "col1",
      "f2" -> "col2",
    )
  }

  test("parseAnInsertWithFieldAliasAndAutocreateWithPKs") {
    val topic = "TOPIC_A"
    val table = "TABLE_A"
    val syntax =
      s"INSERT INTO $table SELECT f1 as col1, f2 as col2, col3 FROM $topic AUTOCREATE PK col1,col3"
    val kcql = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)

    kcql.isAutoCreate should be(true)
    kcql.getWriteMode should be(WriteModeEnum.INSERT)
    kcql.isAutoEvolve should be(false)

    val map = kcql.getFields.asScala.map(alias => alias.getName -> alias.getAlias).toMap
    map should have size 3
    map should contain allOf (
      "f1"   -> "col1",
      "f2"   -> "col2",
      "col3" -> "col3",
    )

    val pks = kcql.getPrimaryKeys.asScala.map(_.toString)
    pks should have size 2
    pks should contain allOf ("col1", "col3")

  }
  test("parseAnInsertWithFieldAliasAndAutocreateWithPKsAndAutoevolve") {
    val topic = "TOPIC_A"
    val table = "TABLE_A"
    val syntax =
      s"INSERT INTO $table SELECT f1 as col1, f2 as col2, col3 FROM $topic AUTOCREATE PK col1,col3 AUTOEVOLVE"
    val kcql = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)

    val map = kcql.getFields.asScala.map(alias => alias.getName -> alias.getAlias).toMap
    map should have size 3
    map should contain allOf (
      "f1"   -> "col1",
      "f2"   -> "col2",
      "col3" -> "col3",
    )

    kcql.isAutoCreate should be(true)

    val pks = kcql.getPrimaryKeys.asScala.map(_.toString)
    pks should have size 2
    pks should contain allOf ("col1", "col3")

    kcql.getWriteMode should be(WriteModeEnum.INSERT)
    kcql.isAutoEvolve should be(true)
  }

  test("handlerPartitionByWhenAllFieldsAreIncluded") {
    val topic = "TOPIC_A"
    val table = "TABLE_A"
    val syntax =
      s"UPSERT INTO $table SELECT * FROM $topic IGNORE col1, 1col2 PARTITIONBY col1,col2  "
    val kcql        = Kcql.parse(syntax)
    val partitionBy = kcql.getPartitionBy.asScala.toSet
    partitionBy should have size 2
    partitionBy should contain allOf ("col1", "col2")
  }

  test("handlerPartitionByFromHeader") {
    val topic       = "TOPIC_A"
    val table       = "TABLE_A"
    val syntax      = s"UPSERT INTO $table SELECT * FROM $topic IGNORE col1, 1col2 PARTITIONBY _header.col1,_header.col2  "
    val kcql        = Kcql.parse(syntax)
    val partitionBy = kcql.getPartitionBy.asScala.toSet
    partitionBy should have size 2
    partitionBy should contain allOf ("_header.col1", "_header.col2")
  }

  test("partitionByShouldAllowQuotingGroupsOfFields") {
    val topic = "TOPIC_A"
    val table = "TABLE_A"
    val syntax =
      s"UPSERT INTO $table SELECT * FROM $topic IGNORE col1, 1col2 PARTITIONBY _header.cost.centre.id,_header.`cost.centre.id`  "
    val kcql        = Kcql.parse(syntax)
    val partitionBy = kcql.getPartitionBy.asScala.toSet
    partitionBy should have size 2
    partitionBy should contain allOf ("_header.cost.centre.id", "_header.`cost.centre.id`")
  }

  test("handlerPartitionByWhenSpecificFieldsAreIncluded") {
    val topic       = "TOPIC_A"
    val table       = "TABLE_A"
    val syntax      = s"UPSERT INTO $table SELECT col1, col2, col3 FROM $topic IGNORE col1, 1col2 PARTITIONBY col1,col2  "
    val kcql        = Kcql.parse(syntax)
    val partitionBy = kcql.getPartitionBy.asScala.toSet
    partitionBy should have size 2
    partitionBy should contain allOf ("col1", "col2")

  }
  test("handlerPartitionByWhenSpecificFieldsAreIncludedAndAliasingIsPresent") {
    val topic = "TOPIC_A"
    val table = "TABLE_A"
    val syntax =
      s"UPSERT INTO $table SELECT col1, col2 as colABC, col3 FROM $topic IGNORE col1, 1col2 PARTITIONBY col1,colABC "
    val kcql        = Kcql.parse(syntax)
    val partitionBy = kcql.getPartitionBy.asScala.toSet
    partitionBy should have size 2
    partitionBy should contain allOf ("col1", "colABC")
  }

  test("handleTimestampAsOneOfTheFields") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT col1,col2 FROM $topic WITHTIMESTAMP col1"
    val kcql   = Kcql.parse(syntax)
    kcql.getTimestamp should be("col1")
  }
  test("handleTypeAsOneOfTheFields") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT col1,col2 FROM $topic WITHTYPE QUEUE"
    val kcql   = Kcql.parse(syntax)
    kcql.getWithType should be("QUEUE")
  }
  test("handleCompoundWITHFields") {
    val topic = "TOPIC_A"
    val table = "TABLE_A"
    val syntax =
      s"INSERT INTO $topic SELECT col1,col2 FROM $table WITHTYPE QUEUE WITHCONVERTER=`com.blah.Converter` WITHJMSSELECTOR=`apples > 10`"
    val kcql = Kcql.parse(syntax)
    kcql.getWithType should be("QUEUE")
    kcql.getWithConverter should be("com.blah.Converter")
    kcql.getWithJmsSelector should be("apples > 10")
  }

  test("handleTimestampWhenAllFieldIncluded") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT * FROM $topic WITHTIMESTAMP col1"
    val kcql   = Kcql.parse(syntax)
    kcql.getTimestamp should be("col1")
  }

  test("handleTimestampSetAsCurrentSysWhenAllFieldsIncluded") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $topic SELECT * FROM $table WITHTIMESTAMP ${Kcql.TIMESTAMP}"
    val kcql   = Kcql.parse(syntax)
    kcql.getTimestamp should be(Kcql.TIMESTAMP)
  }

  test("handleFieldSelectionWithPKWithTimestampSetAsFieldNotInSelection") {
    val syntax =
      "INSERT INTO measurements SELECT actualTemperature, targetTemperature FROM TOPIC_A PK machineId, type WITHTIMESTAMP ts"
    val kcql = Kcql.parse(syntax)
    kcql.getTimestamp should be("ts")
    val pks = kcql.getPrimaryKeys.asScala.map(_.toString)
    pks.size should be(2)
    pks should contain allOf ("type", "machineId")
  }

  test("handleTimestampSetAsCurrentSysWhenSelectedFieldsIncluded") {
    val topic = "TOPIC_A"
    val table = "TABLE_A"
    val syntax =
      s"INSERT INTO $table SELECT col1, col2,col3 FROM $topic WITHTIMESTAMP ${Kcql.TIMESTAMP},"

    val kcql = Kcql.parse(syntax)
    kcql.getTimestamp should be(Kcql.TIMESTAMP)
  }

  test("handleAtCharacterInFields") {
    val topic = "TOPIC_A"
    val table = "TABLE_A"
    val syntax =
      s"INSERT INTO $table SELECT @col1, col2,col3 FROM $topic WITHTIMESTAMP ${Kcql.TIMESTAMP}}"
    val kcql = Kcql.parse(syntax)
    kcql.getFields.get(0).getName should be("@col1")
  }

  test("handleKeyDelimeter") {
    val syntax = "INSERT INTO abc SELECT @col1, col2,col3 FROM %s KEYDELIMITER ='|'"
    val kcql   = Kcql.parse(syntax)
    kcql.getKeyDelimeter should be("|")
  }

  test("handleKeyDelimeterSelect") {
    val syntax = "SELECT @col1, col2,col3 FROM topic KEYDELIMITER ='|'"
    val kcql   = Kcql.parse(syntax)
    kcql.getKeyDelimeter should be("|")
  }

  test("handleWithKey") {
    val topic    = "TOPIC_A"
    val table    = "TABLE_A"
    val syntax   = s"INSERT INTO $topic SELECT @col1, col2,col3 FROM $table WITHKEY(col1, col2, col3)"
    val kcql     = Kcql.parse(syntax)
    val withKeys = kcql.getWithKeys
    withKeys should have size 3
    withKeys should contain inOrder ("col1", "col2", "col3")
  }

  test("handleWithKeyEscaped") {
    val topic    = "TOPIC_A"
    val table    = "TABLE_A"
    val syntax   = s"INSERT INTO $topic SELECT @col1, col2,col3 FROM $table WITHKEY(`col1`, `col2`)"
    val kcql     = Kcql.parse(syntax)
    val withKeys = kcql.getWithKeys
    withKeys.get(0) should be("col1")
    withKeys.get(1) should be("col2")
    withKeys should have size 2
  }

  test("handleStoredAs") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT * FROM $topic WITHFORMAT avro"
    val kcql   = Kcql.parse(syntax)
    kcql.getFormatType.toString should be("AVRO")
    val syntax2 = s"INSERT INTO $table SELECT * FROM $topic WITHFORMAT json"
    val c2      = Kcql.parse(syntax2)
    c2.getFormatType.toString should be("JSON")
    val syntax3 = s"INSERT INTO $table SELECT * FROM $topic WITHFORMAT map"
    val c3      = Kcql.parse(syntax3)
    c3.getFormatType.toString should be("MAP")
    val syntax4 = s"INSERT INTO $table SELECT * FROM $topic WITHFORMAT object"
    val c4      = Kcql.parse(syntax4)
    c4.getFormatType.toString should be("OBJECT")
    val syntax5 = s"INSERT INTO $table SELECT * FROM $topic WITHFORMAT protobuf"
    val c5      = Kcql.parse(syntax5)
    c5.getFormatType.toString should be("PROTOBUF")
  }

  test("throwExceptionIfStoredAsTypeIsMissing") {
    assertThrows[IllegalArgumentException] {
      val topic  = "TOPIC_A"
      val table  = "TABLE_A"
      val syntax = s"UPSERT INTO $table SELECT col1,col2 FROM $topic STOREAS"
      Kcql.parse(syntax)
    }
  }

  test("throwAnExceptionIfStoredAsParametersIsEmpty") {
    assertThrows[IllegalArgumentException] {
      val topic  = "TOPIC_A"
      val table  = "TABLE_A"
      val syntax = s"UPSERT INTO $table SELECT col1,col2 FROM $topic STOREAS SS ()"
      Kcql.parse(syntax)
    }
  }

  test("throwAnExceptionIfStoredAsParameterAppersTwice") {
    assertThrows[IllegalArgumentException] {
      val topic  = "TOPIC_A"
      val table  = "TABLE_A"
      val syntax = s"UPSERT INTO $table SELECT col1,col2 FROM $topic STOREAS SS (name = something , NaMe= something)"
      Kcql.parse(syntax)
    }
  }

  test("handleStoredAsClause") {
    val topic = "TOPIC_A"
    val table = "TABLE_A"
    val syntax =
      s"INSERT INTO $table SELECT col1,col2 FROM $topic STOREAS SS (param1 = value1 , param2 = value2,param3=value3)"
    val kcql = Kcql.parse(syntax)
    kcql.getStoredAs should be("SS")
    kcql.getStoredAsParameters should have size 3
    kcql.getStoredAsParameters.asScala should contain allOf (
      "param1" -> "value1",
      "param2" -> "value2",
      "param3" -> "value3"
    )
  }

  test("handleSemicolonInTarget") {
    val topic  = "TOPIC_A"
    val table  = "namespace1:TABLE_A"
    val syntax = s"INSERT INTO $table SELECT col1,col2 FROM $topic"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
  }

  test("handleForwardSlashInSource") {
    val topic  = "/TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT col1,col2 FROM $topic"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
  }

  test("handleTimestampUnit") {
    val topic  = "/TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT col1,col2 FROM $topic TIMESTAMPUNIT=SECONDS"
    val kcql   = Kcql.parse(syntax)
    kcql.getTimestampUnit should be(TimeUnit.SECONDS)
  }

  test("handleWithTarget") {
    val topic  = "/TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT col1,col2 FROM $topic WITHTARGET = field1.field2.field3 WITHFORMAT object"
    val kcql   = Kcql.parse(syntax)
    kcql.getDynamicTarget should be("field1.field2.field3")
  }

  test("parseTags") {
    val topic = "TOPIC_A"
    val table = "TABLE_A"
    val syntax =
      s"INSERT INTO $table SELECT * FROM $topic WITHTAG (field1, c1=v1, field2, c2=v2, field1.field2 as namedTag)"
    val kcql = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getFields should not be empty
    kcql.getFields.get(0).getName should be("*")
    kcql.getWriteMode should be(WriteModeEnum.INSERT)
    val tagsMap = kcql.getTags.asScala.map(tag => tag.getKey -> tag.getType).toMap
    tagsMap.size should be(5)
    tagsMap should contain allOf (
      "field1"        -> Tag.TagType.DEFAULT,
      "field2"        -> Tag.TagType.DEFAULT,
      "c2"            -> Tag.TagType.CONSTANT,
      "c1"            -> Tag.TagType.CONSTANT,
      "field1.field2" -> Tag.TagType.ALIAS,
    )
  }

  test("parseTagsWithNestedFields") {
    val topic = "TOPIC_A"
    val table = "TABLE_A"
    val syntax =
      s"INSERT INTO $table SELECT * FROM $topic WITHTAG (field1.fieldA, c1=v1, field2, c2=v2)"
    val kcql = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getFields should not be empty
    kcql.getFields.get(0).getName should be("*")
    kcql.getWriteMode should be(WriteModeEnum.INSERT)
    val tagsMap = kcql.getTags.asScala.map(tag => tag.getKey -> tag.getType).toMap
    tagsMap.size should be(4)
    tagsMap should contain allOf (
      "field1.fieldA" -> Tag.TagType.DEFAULT,
      "field2"        -> Tag.TagType.DEFAULT,
      "c2"            -> Tag.TagType.CONSTANT,
      "c1"            -> Tag.TagType.CONSTANT,
    )
  }

  test("throwExceptionWhenTagsWithNestedFieldsEndsWithDot") {
    assertThrows[IllegalArgumentException] {
      val topic = "TOPIC_A"
      val table = "TABLE_A"
      val syntax =
        s"INSERT INTO $table SELECT * FROM $topic WITHTAG (field1.fieldA., c1=v1, field2, c2=v2)"
      Kcql.parse(syntax)
    }
  }

  test("throwAnExceptionIfTagsListIsEmpty") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"UPSERT INTO $table SELECT col1,col2 FROM $topic WITHTAGS ()"
    val kcql   = Kcql.parse(syntax)
    kcql.getTags should be(null)
  }

  test("handleWithPipeline") {
    val topic = "/TOPIC_A"
    val table = "TABLE_A"
    val syntax =
      s"INSERT INTO $table SELECT col1,col2 FROM $topic WITHPIPELINE = field1.field2.field3"
    val kcql = Kcql.parse(syntax)
    kcql.getPipeline should be("field1.field2.field3")
  }

  test("handleWithSubscription") {
    val syntax = "INSERT INTO A SELECT * FROM B WITHSUBSCRIPTION = shared"
    val kcql   = Kcql.parse(syntax)
    kcql.getWithSubscription should be("shared")
  }

  test("handleWithPartitioner") {
    val syntax = "INSERT INTO A SELECT * FROM B WITHPARTITIONER = shared"
    val kcql   = Kcql.parse(syntax)
    kcql.getWithPartitioner should be("shared")
  }

  test("handleWithRegex") {
    val topic = "/TOPIC_A"
    val table = "TABLE_A"
    val syntax =
      s"INSERT INTO $table SELECT col1,col2 FROM $topic WITHCONVERTER=`com.blah.Converter` WITHREGEX=`/^#?([a-f0-9]{6}|[a-f0-9]{3})$$/`"
    val kcql = Kcql.parse(syntax)
    kcql.getWithRegex should be("/^#?([a-f0-9]{6}|[a-f0-9]{3})$/")
    kcql.getWithConverter should be("com.blah.Converter")
  }

  test("handleWithFlushInterval") {
    val topic  = "/TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT col1,col2 FROM $topic WITH_FLUSH_INTERVAL = 2010"
    val kcql   = Kcql.parse(syntax)
    kcql.getWithFlushInterval should be(2010)
  }

  test("throwExceptionWithFlushInterval") {
    assertThrows[IllegalArgumentException] {
      val topic  = "TOPIC_A"
      val table  = "TABLE_A"
      val syntax = s"INSERT INTO $table SELECT col1,col2 FROM $topic WITH_FLUSH_INTERVAL = 0"
      Kcql.parse(syntax)
    }
  }

  test("handleWithSize") {
    val topic  = "/TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT col1,col2 FROM $topic WITH_FLUSH_SIZE = 2010"
    val kcql   = Kcql.parse(syntax)
    kcql.getWithFlushSize should be(2010)
  }

  test("throwExceptionWithSize") {
    assertThrows[IllegalArgumentException] {
      val topic  = "TOPIC_A"
      val table  = "TABLE_A"
      val syntax = s"INSERT INTO $table SELECT col1,col2 FROM $topic WITH_FLUSH_SIZE = 0"
      Kcql.parse(syntax)
    }
  }
  test("handleWithCount") {
    val topic  = "/TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT col1,col2 FROM $topic WITH_FLUSH_COUNT = 2010"
    val kcql   = Kcql.parse(syntax)
    kcql.getWithFlushCount should be(2010)
  }

  test("throwExceptionWithCount") {
    assertThrows[IllegalArgumentException] {
      val topic  = "TOPIC_A"
      val table  = "TABLE_A"
      val syntax = s"INSERT INTO $table SELECT col1,col2 FROM $topic WITH_FLUSH_COUNT = 0"
      Kcql.parse(syntax)
    }
  }

  test("handleTTL") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT col1,col2 FROM $topic TTL=1"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTTL should be(1)
  }
  test("handleTTLSelectOnly") {
    val table  = "TABLE_A"
    val syntax = s"SELECT * FROM ${table}PK sensorID STOREAS SortedSet(score=ts) TTL = 60"
    val kcql   = Kcql.parse(syntax)
    kcql.getTTL should be(60)
  }

  test("handleLimit") {
    val syntax = "insert into mytopic select a from mytable limit 200"
    val kcql   = Kcql.parse(syntax)
    kcql.getLimit should be(200)

  }

  test("handleUpdate") {
    val syntax = "update into mytopic select a, b, c from topic"
    val kcql   = Kcql.parse(syntax)
    kcql.getWriteMode should be(WriteModeEnum.UPDATE)
  }

  test("handleKeys") {
    val syntax = "insert into target select _key.a, _key.p.c, value_field, _header.h FROM topic"
    val kcql   = Kcql.parse(syntax)
    kcql.getKeyFields should have size 2
    kcql.getKeyFields.get(0).getName should be("a")
    kcql.getKeyFields.get(1).getName should be("c")
    kcql.getKeyFields.get(1).getParentFields.get(0) should be("p")
    kcql.getHeaderFields.get(0).getName should be("h")
    kcql.getFields.get(0).getName should be("value_field")
  }

  test("handleKeysAll") {
    val syntax = "insert into target select _key.* FROM topic"
    val kcql   = Kcql.parse(syntax)
    kcql.getKeyFields.size should be(1)
  }

  test("handleHeaders") {
    val syntax = "insert into target select _header.a, _header.p.c, value_field FROM topic"
    val kcql   = Kcql.parse(syntax)
    kcql.getHeaderFields should have size 2
    kcql.getHeaderFields.asScala.map(_.getName) should contain inOrder ("a", "c")
    kcql.getHeaderFields.size should be(2)
    kcql.getHeaderFields.get(1).getParentFields.get(0) should be("p")
    kcql.getFields.get(0).getName should be("value_field")
  }
}
