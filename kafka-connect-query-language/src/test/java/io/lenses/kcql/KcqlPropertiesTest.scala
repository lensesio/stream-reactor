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
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters.ListHasAsScala

class KcqlPropertiesTest extends AnyFunSuite {

  test("emptyPropertiesIfNotDefined") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT * FROM $topic PK f1,f2"
    val kcql   = Kcql.parse(syntax)

    kcql.getProperties should be(empty)
  }

  test("emptyPropertiesIfEmpty") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT * FROM $topic PK f1,f2 properties()"
    val kcql   = Kcql.parse(syntax)

    kcql.getProperties should be(empty)
  }

  test("captureThePropertiesSet") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT * FROM $topic PK f1,f2 properties(a=23, b='xyz')"
    val kcql   = Kcql.parse(syntax)
    kcql.getSource should be(topic)
    kcql.getTarget should be(table)
    kcql.getFields.isEmpty should be(false)
    kcql.getFields.get(0).getName should be("*")
    kcql.getWriteMode should be(WriteModeEnum.INSERT)
    val pks = kcql.getPrimaryKeys.asScala.map(_.toString).toSet

    pks should have size 2
    pks should contain("f1")
    pks should contain("f2")
    kcql.getTags should be(null)
    kcql.isUnwrapping should be(false)
    kcql.getProperties should have size 2
    kcql.getProperties.get("a") should be("23")
    kcql.getProperties.get("b") should be("xyz")
  }

  test("capturePropertyValueWithWithSpace") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT * FROM $topic PK f1,f2 properties(a=23, b='xyz', c='a b c')"
    val kcql   = Kcql.parse(syntax)

    kcql.getProperties should have size 3
    kcql.getProperties.get("a") should be("23")
    kcql.getProperties.get("b") should be("xyz")
    kcql.getProperties.get("c") should be("a b c")
  }

  test("handleEmptyPropertyValue") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT * FROM $topic PK f1,f2 properties(a=23, b='xyz', c='a b c', d='')"
    val kcql   = Kcql.parse(syntax)

    kcql.getProperties should have size 4
    kcql.getProperties.get("a") should be("23")
    kcql.getProperties.get("b") should be("xyz")
    kcql.getProperties.get("c") should be("a b c")
    kcql.getProperties.get("d") should be("")
  }

  test("handlePropertyKeyWithDot") {
    val topic  = "TOPIC_A"
    val table  = "TABLE_A"
    val syntax = s"INSERT INTO $table SELECT * FROM $topic PK f1,f2 properties(a=23, b='xyz', 'c.d'='a b c', d='')"
    val kcql   = Kcql.parse(syntax)

    kcql.getProperties should have size 4
    kcql.getProperties.get("a") should be("23")
    kcql.getProperties.get("b") should be("xyz")
    kcql.getProperties.get("c.d") should be("a b c")
    kcql.getProperties.get("d") should be("")
  }
}
