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

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.sksamuel.elastic4s.http.ElasticDsl._
import io.lenses.streamreactor.common.errors.FatalConnectException
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import io.lenses.streamreactor.connect.elastic6.config.ElasticConfig
import io.lenses.streamreactor.connect.elastic6.config.ElasticConfigConstants._
import io.lenses.streamreactor.connect.elastic6.config.ElasticSettings
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.BeforeAndAfterAll

/**
 * Integration tests for ES6 bulk item-error handling against a real Elasticsearch 6 container.
 *
 * Mapping conflict is induced by mapping `field` as integer then inserting a string.
 */
class ElasticBulkErrorsIT extends ITBase with BeforeAndAfterAll {

  private lazy val node   = CreateLocalNodeClientUtil.createLocalNode()
  private lazy val client = CreateLocalNodeClientUtil.createLocalNodeClient(node)

  override def afterAll(): Unit = {
    client.close()
    node.close()
  }

  private def numericDoc(value: Int) =
    JsonNodeFactory.instance.objectNode().put("field", value)

  private def stringDoc(value: String) =
    JsonNodeFactory.instance.objectNode().put("field", value)

  private def createIndexWithIntegerMapping(index: String): Unit = {
    val create = client.execute {
      createIndex(index).mappings(mapping(index).fields(Seq(intField("field"))))
    }.await
    if (create.isError) {
      throw new IllegalStateException(s"Failed to create index $index: ${create.error.reason}")
    }
  }

  private def settingsFor(index: String) = {
    val props = Map(
      HOSTS                       -> "localhost",
      PROTOCOL                    -> PROTOCOL_DEFAULT,
      KCQL                        -> s"INSERT INTO $index SELECT * FROM topic PK id",
      ERROR_POLICY_CONFIG         -> "THROW",
      BULK_STRICT_ITEM_ERRORS_KEY -> "true",
    )
    ElasticSettings(ElasticConfig(props))
  }

  "strict mode" should {
    "return BulkResult.errors=true for mapper_parsing_exception" in {
      val index = s"strict-map-${System.nanoTime()}"
      createIndexWithIntegerMapping(index)
      val bulk = KElastic6BulkClient(new HttpKElasticClient(client), settingsFor(index))

      bulk.bulk(Seq(InsertOp(index, "1", numericDoc(42), None, None))).get.errors shouldBe false

      val bad = bulk.bulk(Seq(InsertOp(index, "2", stringDoc("not-a-number"), None, None)))
      bad.isSuccess shouldBe true
      bad.get.errors shouldBe true
      bad.get.itemErrors should not be empty
      bad.get.itemErrors.head.errorType.toLowerCase should include("mapper")
    }
  }

  "tolerant mode" should {
    "swallow mapper_parsing_exception and return errors=false" in {
      val index = s"tolerant-map-${System.nanoTime()}"
      createIndexWithIntegerMapping(index)
      val settings = settingsFor(index) match {
        case s => s.copy(strictItemErrors = false)
      }
      val bulk = KElastic6BulkClient(new HttpKElasticClient(client), settings)
      val _    = bulk.bulk(Seq(InsertOp(index, "1", numericDoc(99), None, None)))

      val result = bulk.bulk(Seq(InsertOp(index, "2", stringDoc("still-wrong"), None, None)))
      result.isSuccess shouldBe true
      result.get.errors shouldBe false
      result.get.itemErrors shouldBe empty
    }
  }

  "JsonBulkWriter in strict mode with THROW" should {
    "raise FatalConnectException on a mapping conflict" in {
      val index = s"writer-throw-${System.nanoTime()}"
      createIndexWithIntegerMapping(index)
      val _ = client.execute(indexInto(index / index).id("seed").source("""{"field":1}""")).await

      val settings = settingsFor(index)
      val writer   = new JsonBulkWriter(KElastic6BulkClient(new HttpKElasticClient(client), settings), settings)

      val schema: Schema =
        SchemaBuilder.struct()
          .field("id", Schema.STRING_SCHEMA)
          .field("field", Schema.STRING_SCHEMA)
          .build()
      val struct: Struct     = new Struct(schema).put("id", "bad").put("field", "not-a-number")
      val record: SinkRecord = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", schema, struct, 1L)

      val ex = intercept[FatalConnectException](writer.write(Vector(record)))
      ex.getMessage should include("item-level error")
    }
  }
}
