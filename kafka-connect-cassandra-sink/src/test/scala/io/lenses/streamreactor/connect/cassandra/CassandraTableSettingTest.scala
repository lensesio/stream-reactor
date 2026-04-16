/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cassandra

import io.lenses.kcql.Kcql
import org.apache.kafka.common.config.ConfigException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class CassandraTableSettingTest extends AnyFunSuite with Matchers {

  test("extracts namespace, table, topic, field mappings, and others from simple KCQL") {
    val kcql    = Kcql.parse("INSERT INTO ns1.tbl1 SELECT * FROM topicA PROPERTIES('behavior.on.null.values'='IGNORE')")
    val setting = CassandraTableSetting.fromKcql(kcql)
    setting.namespace shouldBe "ns1"
    setting.table shouldBe "tbl1"
    setting.sourceKafkaTopic shouldBe "topicA"
    setting.fieldMappings.isEmpty shouldBe true
    setting.others should contain("behavior.on.null.values" -> "IGNORE")
  }

  test("extracts namespace and table with quoted target (should reject as invalid)") {
    val kcql = Kcql.parse("INSERT INTO `value.customer.name` SELECT * FROM topicA")
    // Quoted and dotted names are invalid per Cassandra rules
    an[ConfigException] should be thrownBy CassandraTableSetting.fromKcql(kcql)
  }

  test("extracts namespace and table with underscores and dots (should reject as invalid)") {
    val kcql = Kcql.parse("INSERT INTO _value.name.firstName SELECT * FROM topicA")
    // Leading underscore is invalid for namespace, and dot in table is invalid
    an[ConfigException] should be thrownBy CassandraTableSetting.fromKcql(kcql)
  }

  test("rejects table and namespace names with invalid characters") {
    val kcql = Kcql.parse("INSERT INTO bad-ns.tbl SELECT * FROM topicA")
    an[ConfigException] should be thrownBy CassandraTableSetting.fromKcql(kcql)
    val kcql2 = Kcql.parse("INSERT INTO `ns1`.`bad$table` SELECT * FROM topicA")
    an[ConfigException] should be thrownBy CassandraTableSetting.fromKcql(kcql2)
    val kcql3 = Kcql.parse("INSERT INTO bad ns.not-good-table SELECT * FROM topicA")
    an[ConfigException] should be thrownBy CassandraTableSetting.fromKcql(kcql3)
    val kcql4 = Kcql.parse("INSERT INTO ns1.good.table SELECT * FROM topicA")
    an[ConfigException] should be thrownBy CassandraTableSetting.fromKcql(kcql4)
  }

  test("extracts multiple KCQLs from parseMultiple with valid names") {
    val kcqls =
      Kcql.parseMultiple("INSERT INTO abc.tbl SELECT * FROM xyz; INSERT INTO abc.tbl2 SELECT * FROM zyx;").asScala
    val settings = kcqls.map(CassandraTableSetting.fromKcql)
    settings.map(_.namespace) should contain only "abc"
    settings.map(_.table) should contain allOf ("tbl", "tbl2")
    settings.map(_.sourceKafkaTopic) should contain allOf ("xyz", "zyx")
  }

  test("extracts field mappings with aliases and wildcards for valid table name") {
    val kcql = Kcql.parse(
      """INSERT INTO pizza.pizzaavroout
        |SELECT
        |  name,
        |  ingredients_name as fieldName,
        |  calories as cals,
        |  ingredients_sugar as fieldSugar,
        |  ingredients_foo
        |FROM pizza_avro_in""".stripMargin,
    )
    val setting = CassandraTableSetting.fromKcql(kcql)
    setting.namespace shouldBe "pizza"
    setting.table shouldBe "pizzaavroout"
    setting.sourceKafkaTopic shouldBe "pizza_avro_in"
    setting.fieldMappings.exists(f => f.from == "name" && f.to == "name") shouldBe true
    setting.fieldMappings.exists(f => f.from == "ingredients_name" && f.to == "fieldName") shouldBe true
    setting.fieldMappings.exists(f => f.from == "calories" && f.to == "cals") shouldBe true
    setting.fieldMappings.exists(f => f.from == "ingredients_sugar" && f.to == "fieldSugar") shouldBe true
    setting.fieldMappings.exists(f => f.from == "ingredients_foo" && f.to == "ingredients_foo") shouldBe true
  }

  test("rejects table and namespace names that do not start with a letter") {
    val kcql = Kcql.parse("INSERT INTO 1badns.tbl SELECT * FROM topicA")
    an[ConfigException] should be thrownBy CassandraTableSetting.fromKcql(kcql)
    val kcql2 = Kcql.parse("INSERT INTO ns1.1badtable SELECT * FROM topicA")
    an[ConfigException] should be thrownBy CassandraTableSetting.fromKcql(kcql2)
  }

  test("rejects table and namespace names longer than 48 characters") {
    val longName = "a" * 49
    val kcql     = Kcql.parse(s"INSERT INTO $longName.tbl SELECT * FROM topicA")
    an[ConfigException] should be thrownBy CassandraTableSetting.fromKcql(kcql)
    val kcql2 = Kcql.parse(s"INSERT INTO ns1.${longName} SELECT * FROM topicA")
    an[ConfigException] should be thrownBy CassandraTableSetting.fromKcql(kcql2)
  }

  test("accepts valid table and namespace names, case-insensitive") {
    val kcql    = Kcql.parse("INSERT INTO MyKeyspace.MyTable SELECT * FROM topicA")
    val setting = CassandraTableSetting.fromKcql(kcql)
    setting.namespace shouldBe "MyKeyspace"
    setting.table shouldBe "MyTable"
  }

  test("fieldType is 'key' if projection starts with _key") {
    val kcql    = Kcql.parse("INSERT INTO ns1.tbl1 SELECT _key.id as key_id FROM topicA")
    val setting = CassandraTableSetting.fromKcql(kcql)
    setting.fieldMappings should have size 1
    val mapping = setting.fieldMappings.head
    mapping.from shouldBe "id"
    mapping.to shouldBe "key_id"
    mapping.fieldType shouldBe "key"
  }

  test("fieldType is 'header' if projection starts with _header") {
    val kcql    = Kcql.parse("INSERT INTO ns1.tbl1 SELECT _header.myheader as myheader FROM topicA")
    val setting = CassandraTableSetting.fromKcql(kcql)
    setting.fieldMappings should have size 1
    val mapping = setting.fieldMappings.head
    mapping.from shouldBe "myheader"
    mapping.to shouldBe "myheader"
    mapping.fieldType shouldBe "header"
  }

  test("fieldType is 'value' if projection starts with _value or is a regular field") {
    val kcql    = Kcql.parse("INSERT INTO ns1.tbl1 SELECT _value.foo as foo, bar as bar FROM topicA")
    val setting = CassandraTableSetting.fromKcql(kcql)
    setting.fieldMappings should have size 2
    val fooMapping = setting.fieldMappings.find(_.to == "foo").get
    fooMapping.from shouldBe "_value.foo"
    fooMapping.fieldType shouldBe "value"
    val barMapping = setting.fieldMappings.find(_.to == "bar").get
    barMapping.from shouldBe "bar"
    barMapping.fieldType shouldBe "value"
  }

  test("fieldType is 'header' for multiple header fields") {
    val kcql    = Kcql.parse("INSERT INTO ns1.tbl1 SELECT _header.h1 as h1, _header.h2 as h2 FROM topicA")
    val setting = CassandraTableSetting.fromKcql(kcql)
    setting.fieldMappings should have size 2
    setting.fieldMappings.find(_.to == "h1").get.fieldType shouldBe "header"
    setting.fieldMappings.find(_.to == "h2").get.fieldType shouldBe "header"
    setting.fieldMappings.find(_.to == "h1").get.from shouldBe "h1"
    setting.fieldMappings.find(_.to == "h2").get.from shouldBe "h2"
  }

  test("fieldType is correct for mixed key, value, and header projections") {
    val kcql    = Kcql.parse("INSERT INTO `ns1.tbl1` SELECT _key.id as kid, _header.h as h, foo as foo FROM topicA")
    val setting = CassandraTableSetting.fromKcql(kcql)
    setting.fieldMappings.find(_.to == "kid").get.fieldType shouldBe "key"
    setting.fieldMappings.find(_.to == "h").get.fieldType shouldBe "header"
    setting.fieldMappings.find(_.to == "foo").get.fieldType shouldBe "value"
  }

  test("fieldType is 'value' for fields that do not start with _value, _header, or _key") {
    val kcql    = Kcql.parse("INSERT INTO ns1.tbl1 SELECT foo as foo_alias, bar as bar_alias FROM topicA")
    val setting = CassandraTableSetting.fromKcql(kcql)
    setting.fieldMappings should have size 2
    val fooMapping = setting.fieldMappings.find(_.to == "foo_alias").get
    fooMapping.from shouldBe "foo"
    fooMapping.fieldType shouldBe "value"
    val barMapping = setting.fieldMappings.find(_.to == "bar_alias").get
    barMapping.from shouldBe "bar"
    barMapping.fieldType shouldBe "value"
  }
}
