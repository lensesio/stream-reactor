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
package io.lenses.streamreactor.connect.cassandra.adapters

import com.datastax.oss.common.sink.metadata.MetadataCreator
import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import com.datastax.oss.driver.internal.core.`type`.PrimitiveType
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class MetadataTest extends AnyFunSuite with Matchers {

  private val CQL_TYPE               = new PrimitiveType(-1)
  private val JSON_NODE_GENERIC_TYPE = GenericType.of(classOf[JsonNode])

  test("should create metadata for Struct") {
    val schema = SchemaBuilder.struct()
      .name("com.example.Person")
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA)
      .build()
    val obj = new Struct(schema).put("name", "Bobby McGee").put("age", 21)

    val innerDataAndMetadata = MetadataCreator.makeMeta(StructAdapter.maybeAdapt(obj))

    innerDataAndMetadata.getInnerData.getFieldValue("name") shouldBe "Bobby McGee"
    innerDataAndMetadata.getInnerData.getFieldValue("age") shouldBe 21
    innerDataAndMetadata.getInnerMetadata should not be null
    innerDataAndMetadata.getInnerMetadata.getFieldType("name", CQL_TYPE) shouldBe GenericType.STRING
    innerDataAndMetadata.getInnerMetadata.getFieldType("age", CQL_TYPE) shouldBe GenericType.INTEGER
  }

  test("should make metadata for JSON") {
    val json                 = """{"name": "Mike"}"""
    val innerDataAndMetadata = MetadataCreator.makeMeta(json)
    innerDataAndMetadata.getInnerData.getFieldValue("name").asInstanceOf[TextNode].textValue() shouldBe "Mike"
    innerDataAndMetadata.getInnerMetadata should not be null
    innerDataAndMetadata.getInnerMetadata.getFieldType("name", CQL_TYPE) shouldBe GenericType.of(classOf[JsonNode])
  }

  test("should make metadata for enclosed JSON") {
    val json                 = """{"name": {"name2": "Mike"}}"""
    val innerDataAndMetadata = MetadataCreator.makeMeta(json)
    innerDataAndMetadata.getInnerData.getFieldValue("name")
      .asInstanceOf[ObjectNode].get("name2").textValue() shouldBe "Mike"
    innerDataAndMetadata.getInnerMetadata should not be null
    innerDataAndMetadata.getInnerMetadata.getFieldType("name", CQL_TYPE) shouldBe JSON_NODE_GENERIC_TYPE
  }

  test("should treat string literally if it is incorrect JSON") {
    val incorrectJson        = "{name: Mike}"
    val innerDataAndMetadata = MetadataCreator.makeMeta(incorrectJson)
    innerDataAndMetadata.getInnerData.getFieldValue("name") shouldBe incorrectJson
    innerDataAndMetadata.getInnerMetadata should not be null
    innerDataAndMetadata.getInnerMetadata.getFieldType("name", CQL_TYPE) shouldBe GenericType.STRING
  }

  test("should create metadata from Map") {
    val fields               = Map("f_1" -> "v_1").asJava
    val innerDataAndMetadata = MetadataCreator.makeMeta(fields)
    innerDataAndMetadata.getInnerData.getFieldValue("f_1").asInstanceOf[TextNode].textValue() shouldBe "v_1"
    innerDataAndMetadata.getInnerMetadata should not be null
    innerDataAndMetadata.getInnerMetadata.getFieldType("f_1", CQL_TYPE) shouldBe JSON_NODE_GENERIC_TYPE
  }

  test("should create metadata from Map with list field") {
    val fields               = Map("f_1" -> List("1", "2", "3").asJava).asJava
    val innerDataAndMetadata = MetadataCreator.makeMeta(fields)
    val f1Value              = innerDataAndMetadata.getInnerData.getFieldValue("f_1").asInstanceOf[ArrayNode]
    f1Value.get(0).textValue() shouldBe "1"
    f1Value.get(1).textValue() shouldBe "2"
    f1Value.get(2).textValue() shouldBe "3"
    innerDataAndMetadata.getInnerMetadata should not be null
    innerDataAndMetadata.getInnerMetadata.getFieldType("f_1", CQL_TYPE) shouldBe JSON_NODE_GENERIC_TYPE
  }
}
