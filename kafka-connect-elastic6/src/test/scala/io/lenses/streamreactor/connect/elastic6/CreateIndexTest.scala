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
package io.lenses.streamreactor.connect.elastic6

import io.lenses.kcql.Kcql
import io.lenses.streamreactor.connect.elastic6.indexname.CreateIndex
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.EitherValues

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
class CreateIndexTest extends AnyFunSuiteLike with Matchers with EitherValues {
  test("getIndexNameForAutoCreate should create an index name without suffix when suffix not set") {
    val kcql = Kcql.parse("INSERT INTO index_name SELECT * FROM topicA")
    CreateIndex.getIndexNameForAutoCreate(kcql).value shouldBe "index_name"
  }

  test("getIndexNameForAutoCreate should create an index name with suffix when suffix is set") {
    val kcql = Kcql.parse("INSERT INTO index_name SELECT * FROM topicA WITHINDEXSUFFIX=_suffix_{YYYY-MM-dd}")

    val formattedDateTime = LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("YYYY-MM-dd"))
    CreateIndex.getIndexNameForAutoCreate(kcql).value shouldBe s"index_name_suffix_$formattedDateTime"
  }

  test("getIndexNameForAutoCreate should error when requesting a dynamic index") {
    val kcql = Kcql.parse("INSERT INTO _key.sausages SELECT * FROM topicA")

    val ex = CreateIndex.getIndexNameForAutoCreate(kcql).left.value
    ex shouldBe an[IllegalArgumentException]
    ex.getMessage should be("AutoCreate can not be used in conjunction with targets for keys, values or headers")
  }

  test("CreateIndex should create an index name based on SinkRecord and Kcql - static") {
    val kcql       = Kcql.parse("INSERT INTO index_name SELECT * FROM topicA")
    val sinkRecord = new SinkRecord("topicA", 0, null, null, null, null, 0)

    CreateIndex.getIndexName(kcql, sinkRecord).value shouldBe "index_name"
  }

  test("CreateIndex should create an index name based on SinkRecord and Kcql - headers") {
    val kcql    = Kcql.parse("INSERT INTO _header.gate SELECT * FROM topicA")
    val headers = new ConnectHeaders().add("gate", new SchemaAndValue(Schema.INT32_SCHEMA, 55))

    val sinkRecord = new SinkRecord("topicA", 0, null, null, null, null, 0, null, null, headers)

    CreateIndex.getIndexName(kcql, sinkRecord).value shouldBe "55"
  }

  test("CreateIndex should create an index name based on SinkRecord and Kcql - headers with dots") {
    val kcql    = Kcql.parse("INSERT INTO `_header.'prefix.abc.suffix'` SELECT * FROM topicA")
    val headers = new ConnectHeaders().add("prefix.abc.suffix", new SchemaAndValue(Schema.INT32_SCHEMA, 66))

    val sinkRecord = new SinkRecord("topicA", 0, null, null, null, null, 0, null, null, headers)

    CreateIndex.getIndexName(kcql, sinkRecord).value shouldBe "66"
  }

  test("CreateIndex should create an index name based on SinkRecord and Kcql - whole key") {
    val kcql       = Kcql.parse("INSERT INTO _key SELECT * FROM topicA")
    val sinkRecord = new SinkRecord("topicA", 0, Schema.STRING_SCHEMA, "freddie", null, null, 0)

    CreateIndex.getIndexName(kcql, sinkRecord).value shouldBe "freddie"
  }

  test("CreateIndex should create an index name based on SinkRecord and Kcql - path in value") {
    val valueSchema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build()
    val value       = new Struct(valueSchema).put("name", "jason")
    val kcql        = Kcql.parse("INSERT INTO _value.name SELECT * FROM topicA")
    val sinkRecord  = new SinkRecord("topicA", 0, null, null, valueSchema, value, 0)

    CreateIndex.getIndexName(kcql, sinkRecord).value shouldBe "jason"
  }

  test("CreateIndex should create an index name based on SinkRecord and Kcql - nested path in value") {
    val nestedSchema = SchemaBuilder.struct().field("firstName", Schema.STRING_SCHEMA).build()
    val valueSchema  = SchemaBuilder.struct().field("name", nestedSchema).build()
    val name         = new Struct(nestedSchema).put("firstName", "hans")
    val value        = new Struct(valueSchema).put("name", name)

    val kcql       = Kcql.parse("INSERT INTO _value.name.firstName SELECT * FROM topicA")
    val sinkRecord = new SinkRecord("topicA", 0, null, null, valueSchema, value, 0)

    CreateIndex.getIndexName(kcql, sinkRecord).value shouldBe "hans"
  }

  test("CreateIndex should create an index name based on SinkRecord and Kcql - nested path with dots in value") {
    val nestedSchema = SchemaBuilder.struct().field("first.name", Schema.STRING_SCHEMA).build()
    val valueSchema  = SchemaBuilder.struct().field("customer.name", nestedSchema).build()
    val name         = new Struct(nestedSchema).put("first.name", "hans")
    val value        = new Struct(valueSchema).put("customer.name", name)

    val kcql       = Kcql.parse("INSERT INTO `'_value.'customer.name'.'first.name'` SELECT * FROM topicA")
    val sinkRecord = new SinkRecord("topicA", 0, null, null, valueSchema, value, 0)

    CreateIndex.getIndexName(kcql, sinkRecord).value shouldBe "hans"
  }

}
