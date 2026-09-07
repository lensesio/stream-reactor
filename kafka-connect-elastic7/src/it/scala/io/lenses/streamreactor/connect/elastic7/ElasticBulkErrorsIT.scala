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
package io.lenses.streamreactor.connect.elastic7

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.sksamuel.elastic4s.ElasticDsl._
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.elastic.common.config.ItemErrorDlqMode
import io.lenses.streamreactor.connect.elastic.common.dlq.DlqGate
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import io.lenses.streamreactor.connect.elastic7.config.ElasticConfig
import io.lenses.streamreactor.connect.elastic7.config.ElasticConfigConstants._
import io.lenses.streamreactor.connect.elastic7.config.ElasticSettings
import io.lenses.streamreactor.common.errors.FatalConnectException
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.BeforeAndAfterAll

import java.util.concurrent.CompletableFuture
import scala.collection.mutable

/**
 * Integration tests for ES7 bulk item-error handling against a real Elasticsearch 7 container.
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
      createIndex(index).mapping(properties(intField("field")))
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
      val bulk = KElasticBulkClient(new HttpKElasticClient(client), settingsFor(index))

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
      val bulk = KElasticBulkClient(new HttpKElasticClient(client), settings)
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
      val _ = client.execute(indexInto(index).id("seed").source("""{"field":1}""")).await

      val settings = settingsFor(index)
      val writer   = new JsonBulkWriter(KElasticBulkClient(new HttpKElasticClient(client), settings), settings)

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

  // ---- Per-record DLQ against a real cluster (lensesio-dev/stream-reactor#414) -------------

  /** Records every (record, cause) it was asked to report and always acknowledges immediately. */
  private class CapturingReporter extends ErrantRecordReporter {
    val received: mutable.Buffer[(SinkRecord, Throwable)] = mutable.Buffer.empty
    override def report(record: SinkRecord, error: Throwable): java.util.concurrent.Future[Void] = {
      received += (record -> error)
      CompletableFuture.completedFuture[Void](null)
    }
  }

  "JsonBulkWriter in skip mode with a DLQ available" should {
    "report the mapping-conflict record to the DLQ instead of throwing, and still index the good record" in {
      val index = s"writer-skip-${System.nanoTime()}"
      createIndexWithIntegerMapping(index)

      val settings = settingsFor(index).copy(itemErrorDlqMode = ItemErrorDlqMode.Skip)
      val reporter = new CapturingReporter
      val writer = new JsonBulkWriter(
        KElasticBulkClient(new HttpKElasticClient(client), settings),
        settings,
        DlqGate(Some(reporter), dlqTopicConfigured = true),
      )

      val schema: Schema =
        SchemaBuilder.struct()
          .field("id", Schema.STRING_SCHEMA)
          .field("field", Schema.STRING_SCHEMA)
          .build()
      val goodRecord =
        new SinkRecord("topic",
                       0,
                       Schema.STRING_SCHEMA,
                       "k1",
                       schema,
                       new Struct(schema).put("id", "good").put(
                         "field",
                         "123",
                       ),
                       0L,
        )
      val badRecord =
        new SinkRecord(
          "topic",
          0,
          Schema.STRING_SCHEMA,
          "k2",
          schema,
          new Struct(schema).put("id", "bad").put("field", "not-a-number"),
          1L,
        )

      val reported = writer.write(Vector(goodRecord, badRecord))

      reported shouldBe Set(badRecord)
      reporter.received.map(_._1) shouldBe Seq(badRecord)
      reporter.received.head._2.getMessage.toLowerCase should include("mapper")

      // The good record in the same batch was actually written, not swept up by the failure.
      val fetched = client.execute(get(index, "good")).await
      fetched.result.exists shouldBe true
    }
  }

  // ---- Position correlation through a write ALIAS (review finding M5) ---------------------
  //
  // Elasticsearch resolves a write alias to its concrete backing index before writing, and
  // echoes that CONCRETE index in the bulk response `_index` field — never the alias name that
  // was actually sent (confirmed against a real ES7 cluster below). ErrantRecordHandler maps a
  // response item back to its record by position, corroborated by `id` only — NOT `index` —
  // precisely so this alias/concrete-index difference does not cause the mapping to be
  // refused. See ErrantRecordHandler.scala's scaladoc for the full reasoning.
  "JsonBulkWriter in skip mode targeting a write ALIAS" should {
    "correlate correctly by id and report the mapping-conflict record to the DLQ, even though the response reports the concrete backing index, not the alias" in {
      val backingIndex = s"alias-backing-${System.nanoTime()}"
      val aliasName    = s"alias-target-${System.nanoTime()}"
      createIndexWithIntegerMapping(backingIndex)
      client.execute(aliases(addAlias(aliasName, backingIndex))).await

      val props = Map(
        HOSTS                       -> "localhost",
        PROTOCOL                    -> PROTOCOL_DEFAULT,
        KCQL                        -> s"INSERT INTO $aliasName SELECT * FROM topic PK id",
        ERROR_POLICY_CONFIG         -> "THROW",
        BULK_STRICT_ITEM_ERRORS_KEY -> "true",
      )
      val settings = ElasticSettings(ElasticConfig(props)).copy(itemErrorDlqMode = ItemErrorDlqMode.Skip)
      val reporter = new CapturingReporter
      val writer = new JsonBulkWriter(
        KElasticBulkClient(new HttpKElasticClient(client), settings),
        settings,
        DlqGate(Some(reporter), dlqTopicConfigured = true),
      )

      val schema: Schema =
        SchemaBuilder.struct()
          .field("id", Schema.STRING_SCHEMA)
          .field("field", Schema.STRING_SCHEMA)
          .build()
      val goodRecord =
        new SinkRecord("topic",
                       0,
                       Schema.STRING_SCHEMA,
                       "k1",
                       schema,
                       new Struct(schema).put("id", "good").put("field", "123"),
                       0L,
        )
      val badRecord =
        new SinkRecord("topic",
                       0,
                       Schema.STRING_SCHEMA,
                       "k2",
                       schema,
                       new Struct(schema).put("id", "bad").put("field", "not-a-number"),
                       1L,
        )

      // Directly probe what the bulk API returns for _index through the alias, independent of
      // JsonBulkWriter, so a future reader of this test can see the raw evidence.
      val probe = KElasticBulkClient(new HttpKElasticClient(client), settings)
        .bulk(Seq(InsertOp(aliasName, "probe-bad", stringDoc("not-a-number"), None, None)))
        .get
      probe.errors shouldBe true
      val reportedIndex = probe.itemErrors.head.index
      info(s"Bulk response _index through alias '$aliasName' (backing index '$backingIndex') was: '$reportedIndex'")
      // Confirms the premise this test exists to guard against: ES reports the CONCRETE index,
      // not the alias. If a future ES version changed this, the assertions below would still
      // hold either way — id-based mapping works regardless of which index string comes back.
      reportedIndex shouldBe backingIndex

      val reported = writer.write(Vector(goodRecord, badRecord))

      reported shouldBe Set(badRecord)
      reporter.received.map(_._1) shouldBe Seq(badRecord)
      reporter.received.head._2.getMessage.toLowerCase should include("mapper")

      // The good record in the same batch was actually written, not swept up by the failure.
      val fetched = client.execute(get(backingIndex, "good")).await
      fetched.result.exists shouldBe true
    }
  }

  // ---- Batch-wide (non-400) errors are never diverted to the DLQ (review finding M4) ------

  "JsonBulkWriter in skip mode against a write-blocked index" should {
    "fail the task instead of diverting anything to the DLQ, even for otherwise-valid documents" in {
      val index  = s"blocked-${System.nanoTime()}"
      val create = client.execute(createIndex(index)).await
      if (create.isError) {
        throw new IllegalStateException(s"Failed to create index $index: ${create.error.reason}")
      }
      client.execute(updateSettings(index, settings = Map("index.blocks.write" -> "true"))).await

      val props = Map(
        HOSTS                       -> "localhost",
        PROTOCOL                    -> PROTOCOL_DEFAULT,
        KCQL                        -> s"INSERT INTO $index SELECT * FROM topic PK id",
        ERROR_POLICY_CONFIG         -> "THROW",
        BULK_STRICT_ITEM_ERRORS_KEY -> "true",
      )
      val settings = ElasticSettings(ElasticConfig(props)).copy(itemErrorDlqMode = ItemErrorDlqMode.Skip)
      val reporter = new CapturingReporter
      val writer = new JsonBulkWriter(
        KElasticBulkClient(new HttpKElasticClient(client), settings),
        settings,
        DlqGate(Some(reporter), dlqTopicConfigured = true),
      )

      val schema: Schema =
        SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).field("field", Schema.STRING_SCHEMA).build()
      // A perfectly valid document — the block is on the INDEX, not the document.
      val goodRecord =
        new SinkRecord("topic",
                       0,
                       Schema.STRING_SCHEMA,
                       "k1",
                       schema,
                       new Struct(schema).put("id", "good").put("field", "123"),
                       0L,
        )

      val thrown = intercept[FatalConnectException](writer.write(Vector(goodRecord)))
      thrown.getMessage.toLowerCase should include("cluster_block_exception")
      // Nothing was diverted — a batch-wide condition must never partially divert a batch.
      reporter.received shouldBe empty
    }
  }
}
