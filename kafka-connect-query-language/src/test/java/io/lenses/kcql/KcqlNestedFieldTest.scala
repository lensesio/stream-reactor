/*
 * Copyright 2017-2023 Lenses.io Ltd
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

import com.google.common.collect.Lists
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters.ListHasAsScala
class KcqlNestedFieldTest extends AnyFunSuite {

  test("parseNestedField") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT field1.field2.field3 FROM $topic"
    val kcql   = Kcql.parse(syntax)
    val fields = Lists.newArrayList(kcql.getFields)
    fields should have size 1
    fields.get(0).getName should be("field3")
    fields.get(0).getAlias should be("field3")
    fields.get(0).getParentFields should have size 2
    fields.get(0).getParentFields.get(0) should be("field1")
    fields.get(0).getParentFields.get(1) should be("field2")
    fields.get(0).getFieldType should be(FieldType.VALUE)
  }

  test("parseNestedFieldWithAlias") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT field2.field3 as afield FROM $topic"
    val kcql   = Kcql.parse(syntax)
    val fields = Lists.newArrayList(kcql.getFields)
    fields should have size 1
    fields.get(0).getName should be("field3")
    fields.get(0).getParentFields should have size 1
    fields.get(0).getParentFields.get(0) should be("field2")
    fields.get(0).getAlias should be("afield")
    fields.get(0).getFieldType should be(FieldType.VALUE)
  }

  test("parseMultipleFieldsOneWithAliasTheOtherWithoutNestedFieldWithAlias") {
    val topic  = "TOPIC.A"
    val table  = "TABLE/A"
    val syntax = s"INSERT INTO $table SELECT fieldA as A, field1.field2 as B FROM `$topic`"
    val kcql   = Kcql.parse(syntax)
    val fields = Lists.newArrayList(kcql.getFields)
    fields should have size 2
    fields.get(0).getName should be("fieldA")
    fields.get(0).getAlias should be("A")
    fields.get(0).hasParents should be(false)
    fields.get(0).getFieldType should be(FieldType.VALUE)

    fields.get(1).getName should be("field2")
    fields.get(1).getParentFields.get(0) should be("field1")
    fields.get(1).getAlias should be("B")
    fields.get(1).getFieldType should be(FieldType.VALUE)
  }

  test("parseNestedFieldWithStart") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT field1.field2.field3.* FROM $topic"
    val kcql   = Kcql.parse(syntax)
    val fields = Lists.newArrayList(kcql.getFields)
    fields should have size 1
    fields.get(0).getName should be("*")
    fields.get(0).getAlias should be("*")
    fields.get(0).getParentFields should have size 3
    fields.get(0).getFieldType should be(FieldType.VALUE)

    for (i <- 0 until 3) {
      fields.get(0).getParentFields.get(i) should be(s"field${i + 1}")
    }
  }

  test("throwAnExceptionIfTheFieldSelectionHasStartField") {
    assertThrows[IllegalArgumentException] {
      Kcql.parse("INSERT INTO target SELECT field1.field2.*.field3 FROM source")
    }
  }

  test("throwAnExceptionIfTheFieldSelectionStartsWithStar") {
    assertThrows[IllegalArgumentException] {
      Kcql.parse("INSERT INTO target SELECT *.field3 FROM source")
    }
  }

  test("throwAnExceptionIfOnlyUnderscoreIsProvided") {
    assertThrows[IllegalArgumentException] {
      Kcql.parse("INSERT INTO target SELECT _. FROM source")
    }
  }

  test("throwAnExceptionIfFollowingUnderscoreIsNotAMetadataOrKey") {
    assertThrows[IllegalArgumentException] {
      Kcql.parse("INSERT INTO target SELECT _.picachu FROM source")
    }
  }

  test("throwAnExceptionIfNotSpecifyingAnythingWhenUsingKey") {
    assertThrows[IllegalArgumentException] {
      Kcql.parse("INSERT INTO target SELECT _.key FROM source")
    }
  }

  test("throwAnExceptionIfNotSpecifyingAnythingWhenUsingKeyAndDot") {
    assertThrows[IllegalArgumentException] {
      Kcql.parse("INSERT INTO target SELECT _.key. FROM source")
    }
  }

  test("parseKeyFields") {
    val kcql   = Kcql.parse("INSERT INTO target SELECT _.key.field1,_.key.field2.field3.field4, _.key.* FROM source")
    val fields = kcql.getFields
    fields should have size 3

    val fieldsMap = fields.asScala.map(field => field.getName -> field).toMap

    fieldsMap should have size 3

    fieldsMap should contain key "field1"
    val field1 = fieldsMap("field1")
    field1.getName should be("field1")
    field1.getAlias should be("field1")
    field1.hasParents should be(false)
    field1.getFieldType should be(FieldType.KEY)

    fieldsMap should contain key "*"
    val field2 = fieldsMap("*")
    field2.getName should be("*")
    field2.getAlias should be("*")
    field2.hasParents should be(false)
    field2.getFieldType should be(FieldType.KEY)

    fieldsMap should contain key "field4"
    val field3 = fieldsMap("field4")
    field3.getName should be("field4")
    field3.getAlias should be("field4")
    field3.hasParents should be(true)
    field3.getParentFields should have size 2
    field3.getParentFields.get(0) should be("field2")
    field3.getParentFields.get(1) should be("field3")
    field3.getFieldType should be(FieldType.KEY)
  }

  test("throwAnExceptionIfKeySelectWithStartIsAliased") {
    assertThrows[IllegalArgumentException] {
      Kcql.parse(
        "INSERT INTO target SELECT _.key.field1 as F1,_.key.field2.field3.field4 as F2, _.key.* as X FROM source",
      )
    }
  }

  test("parseKeyFieldsWithAlias") {
    val kcql =
      Kcql.parse("INSERT INTO target SELECT _.key.field1 as F1,_.key.field2.field3.field4 as F2, _.key.* FROM source")
    val fields = kcql.getFields
    fields should have size 3

    val fieldsMap = fields.asScala.map(field => field.getName -> field).toMap

    fieldsMap should have size 3

    fieldsMap should contain key "field1"
    val field1 = fieldsMap("field1")
    field1.getName should be("field1")
    field1.getAlias should be("F1")
    field1.hasParents should be(false)
    field1.getFieldType should be(FieldType.KEY)

    fieldsMap should contain key "*"
    val field2 = fieldsMap("*")
    field2.getName should be("*")
    field2.getAlias should be("*")
    field2.hasParents should be(false)
    field2.getFieldType should be(FieldType.KEY)

    fieldsMap should contain key "field4"
    val field3 = fieldsMap("field4")
    field3.getName should be("field4")
    field3.getAlias should be("F2")
    field3.hasParents should be(true)
    field3.getParentFields should have size 2
    field3.getParentFields.get(0) should be("field2")
    field3.getParentFields.get(1) should be("field3")
    field3.getFieldType should be(FieldType.KEY)
  }

  test("parseMetadataFields") {
    val kcql   = Kcql.parse("INSERT INTO target SELECT _.topic,_.partition,_.offset, _.timestamp FROM source")
    val fields = kcql.getFields
    fields should have size 4

    val fieldsMap = fields.asScala.map(field => field.getName -> field).toMap

    fieldsMap should have size 4
    fieldsMap should contain key "topic"
    val field1 = fieldsMap("topic")
    field1.getName should be("topic")
    field1.getAlias should be("topic")
    field1.getFieldType should be(FieldType.TOPIC)

    fieldsMap should contain key "partition"
    val field2 = fieldsMap("partition")
    field2.getName should be("partition")
    field2.getAlias should be("partition")
    field2.getFieldType should be(FieldType.PARTITION)

    fieldsMap should contain key "offset"
    val field3 = fieldsMap("offset")
    field3.getName should be("offset")
    field3.getAlias should be("offset")
    field3.getFieldType should be(FieldType.OFFSET)

    fieldsMap should contain key "timestamp"
    val field4 = fieldsMap("timestamp")
    field4.getName should be("timestamp")
    field4.getAlias should be("timestamp")
    field4.getFieldType should be(FieldType.TIMESTAMP)
  }

  test("parseMetadataFieldsWithAlias") {
    val kcql =
      Kcql.parse("INSERT INTO target SELECT _.topic as T,_.partition as P,_.offset as O, _.timestamp as TS FROM source")
    val fields = kcql.getFields
    fields should have size 4

    val fieldsMap = fields.asScala.map(field => field.getName -> field).toMap

    fieldsMap should have size 4
    fieldsMap should contain key "topic"
    val field1 = fieldsMap("topic")
    field1.getName should be("topic")
    field1.getAlias should be("T")
    field1.getFieldType should be(FieldType.TOPIC)

    fieldsMap should contain key "partition"
    val field2 = fieldsMap("partition")
    field2.getName should be("partition")
    field2.getAlias should be("P")
    field2.getFieldType should be(FieldType.PARTITION)

    fieldsMap should contain key "offset"
    val field3 = fieldsMap("offset")
    field3.getName should be("offset")
    field3.getAlias should be("O")
    field3.getFieldType should be(FieldType.OFFSET)

    fieldsMap should contain key "timestamp"
    val field4 = fieldsMap("timestamp")
    field4.getName should be("timestamp")
    field4.getAlias should be("TS")
    field4.getFieldType should be(FieldType.TIMESTAMP)
  }

  test("parseFieldsWithTheSameNameButDifferentPath") {
    val topic  = "TOPIC.A"
    val table  = "TABLE/A"
    val syntax = s"INSERT INTO $table SELECT fieldA as A, field1.field2.fieldA, fieldx.fieldA as B FROM `$topic`"
    val kcql   = Kcql.parse(syntax)
    val fields = Lists.newArrayList(kcql.getFields)
    fields should have size 3
    fields.get(0).getName should be("fieldA")
    fields.get(0).getAlias should be("A")
    fields.get(0).hasParents should be(false)
    fields.get(0).getFieldType should be(FieldType.VALUE)

    fields.get(1).getName should be("fieldA")
    fields.get(1).getParentFields.get(0) should be("field1")
    fields.get(1).getParentFields.get(1) should be("field2")
    fields.get(1).getAlias should be("fieldA")
    fields.get(1).getFieldType should be(FieldType.VALUE)

    fields.get(2).getName should be("fieldA")
    fields.get(2).getParentFields.get(0) should be("fieldx")
    fields.get(2).getAlias should be("B")
    fields.get(2).hasParents should be(true)
    fields.get(2).getParentFields should have size 1
    fields.get(2).getFieldType should be(FieldType.VALUE)
  }

  test("handleWithStructureQuery") {
    val kcql = Kcql.parse(
      """|INSERT INTO pizza_avro_out
         |SELECT
         |  name,
         |  ingredients.name as fieldName,
         |  calories as cals,
         |  ingredients.sugar as fieldSugar,
         |  ingredients.*
         |FROM pizza_avro_in withstructure""".stripMargin,
    )
    kcql.hasRetainStructure should be(true)
  }

  test("parseWithIncrementalMode") {
    val incMode = "modeA"
    val syntax  = s"SELECT * FROM topicA INCREMENTALMODE=$incMode"
    val kcql    = Kcql.parse(syntax)
    kcql.getIncrementalMode should be(incMode)
  }

  test("parseWithIndexSuffix") {
    var syntax = "SELECT * FROM topicA WITHINDEXSUFFIX=suffix1"
    var kcql   = Kcql.parse(syntax)
    kcql.getIndexSuffix should be("suffix1")

    syntax = "SELECT * FROM topicA WITHINDEXSUFFIX= _{YYYY-MM-dd} "
    kcql   = Kcql.parse(syntax)
    kcql.getIndexSuffix should be("_{YYYY-MM-dd}")

    syntax = "INSERT INTO index_name SELECT * FROM topicA WITHINDEXSUFFIX= _suffix_{YYYY-MM-dd}"
    kcql   = Kcql.parse(syntax)
    kcql.getIndexSuffix should be("_suffix_{YYYY-MM-dd}")
  }

  test("parseIngoreFields") {
    val syntax  = "SELECT * FROM topicA ignore f1, f2.a"
    val kcql    = Kcql.parse(syntax)
    val ignored = kcql.getIgnoredFields
    ignored should have size 2
    ignored.get(0).getName should be("f1")
    ignored.get(1).getName should be("a")
    ignored.get(1).getParentFields.get(0) should be("f2")
  }

  test("parseWithDocType") {
    var syntax = "SELECT * FROM topicA WITHDOCTYPE=document1"
    var kcql   = Kcql.parse(syntax)
    kcql.getDocType should be("document1")

    syntax = "SELECT * FROM topicA WITHDOCTYPE= `document.name` "
    kcql   = Kcql.parse(syntax)
    kcql.getDocType should be("document.name")
    kcql.getWithConverter should be(null)
  }

  test("parseWithConverter") {
    var syntax = "SELECT * FROM topicA WITHCONVERTER=`io.lenses.converter.Mine`"
    var kcql   = Kcql.parse(syntax)
    kcql.getWithConverter should be("io.lenses.converter.Mine")

    syntax = "SELECT * FROM topicA WITHCONVERTER= `io.lenses.ConverterA` "
    kcql   = Kcql.parse(syntax)
    kcql.getWithConverter should be("io.lenses.ConverterA")
  }

  test("parseWithJmsSelector") {
    var syntax = "INSERT INTO out SELECT * FROM topicA WITHJMSSELECTOR=`apples > 10`"
    var kcql   = Kcql.parse(syntax)
    kcql.getWithJmsSelector should be("apples > 10")

    syntax = "INSERT INTO out SELECT * FROM topicA WITHJMSSELECTOR= `apples > 10` "
    kcql   = Kcql.parse(syntax)
    kcql.getWithJmsSelector should be("apples > 10")
  }
}
