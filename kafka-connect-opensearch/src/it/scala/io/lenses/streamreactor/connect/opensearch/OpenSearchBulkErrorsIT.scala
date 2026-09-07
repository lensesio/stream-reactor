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
package io.lenses.streamreactor.connect.opensearch

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.elastic.common.dlq.DlqGate
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.apache.kafka.connect.sink.SinkRecord

import java.util.concurrent.CompletableFuture
import scala.collection.mutable

/**
 * Integration tests for bulk error handling:
 *  - Strict mode: a real `mapper_parsing_exception` surfaces as `BulkResult.errors=true`
 *  - Tolerant mode: same item error is swallowed, `BulkResult.errors=false`
 *  - AUTOCREATE path: writing to a new index when the KCQL carries AUTOCREATE succeeds
 *
 * The mapping conflict is induced by:
 *  1. Explicitly mapping `field` as `integer` on the index.
 *  2. Inserting a document whose `field` value is a String — OpenSearch returns
 *     `mapper_parsing_exception` per item, while the HTTP response is still 200.
 */
class OpenSearchBulkErrorsIT extends ITBase {

  private val host      = "localhost"
  private lazy val port = container.hostNetwork.httpHostAddress.split(":").last.toInt

  private def numericDoc(value: Int) =
    JsonNodeFactory.instance.objectNode().put("field", value)

  private def stringDoc(value: String) =
    JsonNodeFactory.instance.objectNode().put("field", value)

  /** Create an index with `field` explicitly mapped as `integer`. */
  private def createIndexWithIntegerMapping(index: String, kClient: KOpenSearchClient): Unit = {
    val _ = kClient.createIndex(index)
    // Force the mapping via the OpenSearch indices API through our raw client
    val rawClient = {
      import org.opensearch.client.opensearch.OpenSearchClient
      import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder
      import org.apache.hc.core5.http.HttpHost
      val httpHost  = new HttpHost("http", host, port)
      val transport = ApacheHttpClient5TransportBuilder.builder(httpHost).build()
      new OpenSearchClient(transport)
    }
    val intProp = org.opensearch.client.opensearch._types.mapping.Property.of(p => p.integer(i => i))
    val putReq = org.opensearch.client.opensearch.indices.PutMappingRequest.of { b =>
      b.index(index).properties("field", intProp)
    }
    rawClient.indices().putMapping(putReq)
    rawClient._transport().close()
  }

  test("strict mode: item-level mapper_parsing_exception returns BulkResult with errors=true") {
    val index   = s"strict-test-mapping-${System.nanoTime()}"
    val kClient = makeKClient(baseProps(host, port, s"INSERT INTO $index SELECT * FROM topic"))
    createIndexWithIntegerMapping(index, kClient)

    // Good numeric document — establishes the mapping and should succeed
    val good = kClient.bulk(Seq(InsertOp(index, "1", numericDoc(42), None, None)))
    good.isSuccess shouldBe true
    good.get.errors shouldBe false

    // String document — conflicts with the integer mapping → mapper_parsing_exception per item
    val bad = kClient.bulk(Seq(InsertOp(index, "2", stringDoc("not-a-number"), None, None)))
    bad.isSuccess shouldBe true
    bad.get.errors shouldBe true
    bad.get.itemErrors should not be empty
    bad.get.itemErrors.head.reason.toLowerCase should (
      include("mapper_parsing_exception") or include("mapping") or include("parse")
    )
  }

  test("tolerant mode: item-level mapper_parsing_exception is swallowed, BulkResult.errors=false") {
    val index = s"tolerant-test-mapping-${System.nanoTime()}"
    val kClient = makeKClient(baseProps(
      host,
      port,
      s"INSERT INTO $index SELECT * FROM topic",
      Map(BULK_STRICT_ITEM_ERRORS_KEY -> "false"),
    ))
    createIndexWithIntegerMapping(index, kClient)

    val _ = kClient.bulk(Seq(InsertOp(index, "1", numericDoc(99), None, None)))

    // Same conflicting doc — tolerant mode must swallow the item error
    val result = kClient.bulk(Seq(InsertOp(index, "2", stringDoc("still-wrong"), None, None)))
    result.isSuccess shouldBe true
    result.get.errors shouldBe false
    result.get.itemErrors shouldBe empty
  }

  test("write to a non-existent index succeeds when AUTOCREATE is used in KCQL") {
    val index = s"autocreate-test-${System.nanoTime()}"
    val kClient = makeKClient(baseProps(
      host,
      port,
      s"INSERT INTO $index SELECT * FROM topic AUTOCREATE",
    ))
    val result = bulkWrite(kClient, Seq(InsertOp(index, "1", numericDoc(1), None, None)))
    result.errors shouldBe false
  }

  test("C9: strict-mode + error.policy=THROW throws ConnectException on item-level bulk failure via JsonBulkWriter") {
    val index = s"c9-throw-test-${System.nanoTime()}"

    // Step 1: create the index and fix the mapping via a NOOP client first
    val setupClient = makeKClient(baseProps(host, port, s"INSERT INTO $index SELECT * FROM topic"))
    createIndexWithIntegerMapping(index, setupClient)
    // Seed one good record so the mapping is established
    val _ = setupClient.bulk(Seq(InsertOp(index, "seed", numericDoc(1), None, None)))

    // Step 2: Create a JsonBulkWriter backed by a real KOpenSearchClient with THROW + strict mode
    import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
    import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
    import org.opensearch.client.opensearch.OpenSearchClient

    val props = baseProps(
      host,
      port,
      s"INSERT INTO $index SELECT * FROM topic PK id",
      Map(
        BULK_STRICT_ITEM_ERRORS_KEY       -> "true",
        "connect.opensearch.error.policy" -> "THROW",
      ),
    )
    val config    = OpenSearchConfig(props)
    val settings  = OpenSearchSettings(config)
    val transport = OpenSearchTransportFactory.create(settings)
    val osClient  = new OpenSearchClient(transport)
    val kClient   = new KOpenSearchClient(osClient, settings)
    val writer    = new JsonBulkWriter(kClient, settings.common)

    // Step 3: Send a string value in `field` — triggers the integer mapping conflict.
    // The schema must include both `id` (PK) and `field` (the integer-mapped column);
    // SELECT * FROM topic picks up all fields, so the JSON sent to OpenSearch will be
    // {"id":"not-a-number","field":"not-a-number"} which collides with the integer mapping.
    val schema: Schema =
      SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field("field", Schema.STRING_SCHEMA)
        .build()
    val struct: Struct     = new Struct(schema).put("id", "not-a-number").put("field", "not-a-number")
    val record: SinkRecord = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", schema, struct, 1L)

    // C9 assertion: the writer must throw ConnectException (THROW policy) on item-level errors
    val ex = intercept[ConnectException](writer.write(Vector(record)))
    ex.getMessage should include("item-level error")
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

  test(
    "DLQ: dlq mode reports the mapping-conflict record to the DLQ instead of throwing, and still indexes the good record",
  ) {
    val index = s"dlq-skip-test-${System.nanoTime()}"

    val setupClient = makeKClient(baseProps(host, port, s"INSERT INTO $index SELECT * FROM topic"))
    createIndexWithIntegerMapping(index, setupClient)

    import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
    import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
    import org.opensearch.client.opensearch.OpenSearchClient
    import org.opensearch.client.opensearch.core.GetRequest

    val props = baseProps(
      host,
      port,
      s"INSERT INTO $index SELECT * FROM topic PK id",
      Map(
        BULK_STRICT_ITEM_ERRORS_KEY -> "true",
        BULK_ITEM_ERRORS_KEY        -> "dlq",
      ),
    )
    val config    = OpenSearchConfig(props)
    val settings  = OpenSearchSettings(config)
    val transport = OpenSearchTransportFactory.create(settings)
    val osClient  = new OpenSearchClient(transport)
    val kClient   = new KOpenSearchClient(osClient, settings)
    val reporter  = new CapturingReporter
    val writer    = new JsonBulkWriter(kClient, settings.common, DlqGate(Some(reporter), dlqTopicConfigured = true))

    val schema: Schema =
      SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field("field", Schema.STRING_SCHEMA)
        .build()
    val goodRecord =
      new SinkRecord(
        "topic",
        0,
        Schema.STRING_SCHEMA,
        "k1",
        schema,
        new Struct(schema).put("id", "good").put("field", "123"),
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
    reporter.received.head._2.getMessage.toLowerCase should (include("mapper") or include("mapping"))

    // The good record in the same batch was actually written, not swept up by the failure.
    val fetched = osClient.get(GetRequest.of(g => g.index(index).id("good")), classOf[JsonNode])
    fetched.found shouldBe true

    transport.close()
  }

  // ---- Position correlation through a write ALIAS (review finding M5) ---------------------
  //
  // OpenSearch resolves a write alias to its concrete backing index before writing, and echoes
  // that CONCRETE index in the bulk response — never the alias name that was actually sent
  // (confirmed against a real cluster below, matching the ES7 equivalent test). ErrantRecordHandler
  // maps a response item back to its record by position, corroborated by `id` only — NOT
  // `index` — precisely so this alias/concrete-index difference does not cause the mapping to
  // be refused.
  test(
    "ALIAS: dlq mode targeting a write alias correlates correctly by id and reports the mapping-conflict record to the DLQ",
  ) {
    val backingIndex = s"alias-backing-${System.nanoTime()}"
    val aliasName    = s"alias-target-${System.nanoTime()}"

    val setupClient = makeKClient(baseProps(host, port, s"INSERT INTO $backingIndex SELECT * FROM topic"))
    createIndexWithIntegerMapping(backingIndex, setupClient)
    client.indices().putAlias(b => b.index(backingIndex).name(aliasName))

    import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
    import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
    import org.opensearch.client.opensearch.OpenSearchClient
    import org.opensearch.client.opensearch.core.GetRequest

    val props = baseProps(
      host,
      port,
      s"INSERT INTO $aliasName SELECT * FROM topic PK id",
      Map(BULK_STRICT_ITEM_ERRORS_KEY -> "true", BULK_ITEM_ERRORS_KEY -> "dlq"),
    )
    val config    = OpenSearchConfig(props)
    val settings  = OpenSearchSettings(config)
    val transport = OpenSearchTransportFactory.create(settings)
    val osClient  = new OpenSearchClient(transport)
    val kClient   = new KOpenSearchClient(osClient, settings)
    val reporter  = new CapturingReporter
    val writer    = new JsonBulkWriter(kClient, settings.common, DlqGate(Some(reporter), dlqTopicConfigured = true))

    val schema: Schema =
      SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).field("field", Schema.STRING_SCHEMA).build()
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

    // Directly probe what the bulk API returns for `index` through the alias, independent of
    // JsonBulkWriter, so a future reader of this test can see the raw evidence.
    val probe = kClient.bulk(Seq(InsertOp(aliasName, "probe-bad", stringDoc("not-a-number"), None, None))).get
    probe.errors shouldBe true
    val reportedIndex = probe.itemErrors.head.index
    info(s"Bulk response index through alias '$aliasName' (backing index '$backingIndex') was: '$reportedIndex'")
    // Confirms the premise this test exists to guard against: OpenSearch reports the CONCRETE
    // index, not the alias. If a future version changed this, the assertions below would still
    // hold either way — id-based mapping works regardless of which index string comes back.
    reportedIndex shouldBe backingIndex

    val reported = writer.write(Vector(goodRecord, badRecord))

    reported shouldBe Set(badRecord)
    reporter.received.map(_._1) shouldBe Seq(badRecord)
    reporter.received.head._2.getMessage.toLowerCase should (include("mapper") or include("mapping"))

    // The good record in the same batch was actually written, not swept up by the failure.
    val fetched = osClient.get(GetRequest.of(g => g.index(backingIndex).id("good")), classOf[JsonNode])
    fetched.found shouldBe true

    transport.close()
  }

  // ---- Batch-wide (non-400) errors are never diverted to the DLQ (review finding M4) ------

  test(
    "BLOCKED: dlq mode against a write-blocked index fails the task instead of diverting anything to the DLQ",
  ) {
    val index       = s"blocked-${System.nanoTime()}"
    val setupClient = makeKClient(baseProps(host, port, s"INSERT INTO $index SELECT * FROM topic"))
    val _           = setupClient.createIndex(index)
    client.indices().putSettings(b => b.index(index).settings(s => s.blocksWrite(true)))

    import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
    import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
    import org.opensearch.client.opensearch.OpenSearchClient

    val props = baseProps(
      host,
      port,
      s"INSERT INTO $index SELECT * FROM topic PK id",
      Map(BULK_STRICT_ITEM_ERRORS_KEY -> "true", BULK_ITEM_ERRORS_KEY -> "dlq"),
    )
    val config    = OpenSearchConfig(props)
    val settings  = OpenSearchSettings(config)
    val transport = OpenSearchTransportFactory.create(settings)
    val osClient  = new OpenSearchClient(transport)
    val kClient   = new KOpenSearchClient(osClient, settings)
    val reporter  = new CapturingReporter
    val writer    = new JsonBulkWriter(kClient, settings.common, DlqGate(Some(reporter), dlqTopicConfigured = true))

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

    val thrown = intercept[ConnectException](writer.write(Vector(goodRecord)))
    thrown.getMessage.toLowerCase should include("cluster_block_exception")
    // Nothing was diverted — a batch-wide condition must never partially divert a batch.
    reporter.received shouldBe empty

    transport.close()
  }
}
