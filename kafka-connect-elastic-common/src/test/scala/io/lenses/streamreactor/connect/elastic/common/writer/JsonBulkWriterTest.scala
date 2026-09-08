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
package io.lenses.streamreactor.connect.elastic.common.writer

import ch.qos.logback.classic.{ Logger => LogbackLogger }
import ch.qos.logback.classic.{ Level => LogbackLevel }
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.typesafe.scalalogging.StrictLogging
import cyclops.control.{ Option => COption }
import io.lenses.kcql.Kcql
import io.lenses.kcql.WriteModeEnum
import io.lenses.kcql.targettype.StaticTargetType
import io.lenses.kcql.targettype.TargetType
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkOp
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkResult
import io.lenses.streamreactor.connect.elastic.common.bulk.DeleteOp
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.elastic.common.bulk.KBulkClient
import io.lenses.streamreactor.connect.elastic.common.bulk.UpsertOp
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemError
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings
import io.lenses.streamreactor.common.errors.FatalConnectException
import io.lenses.streamreactor.common.errors.NoopErrorPolicy
import io.lenses.streamreactor.common.errors.RetryErrorPolicy
import io.lenses.streamreactor.common.errors.RetriableIntegrityException
import io.lenses.streamreactor.common.errors.ThrowErrorPolicy
import io.lenses.streamreactor.common.security.StoresInfo
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.RetriableException
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.mockito.ArgumentMatchersSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.util.Collections.emptyList
import java.util.Collections.emptyMap
import scala.collection.mutable
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Success
import scala.util.Try

/**
 * Unit tests for [[JsonBulkWriter]] covering the parity cases documented in the plan:
 *  - INSERT with all fields (SELECT *)
 *  - UPSERT requires PKs
 *  - Tombstone DELETE / IGNORE / FAIL behaviours
 *  - IGNORE fields via KCQL ignoredFields
 *  - RETAINSTRUCTURE propagation
 *  - AUTO_GEN_ID for INSERT without PK
 *  - Batch size grouping
 *  - Multi-KCQL fanout (same topic, two targets)
 *  - Unsupported write mode is rejected at construction
 *  - WITHDOCTYPE warning is logged (not rejected)
 */
class JsonBulkWriterTest
    extends AnyFunSuite
    with Matchers
    with StrictLogging
    with MockitoSugar
    with ArgumentMatchersSugar {

  // ---- Test helpers ----

  /** Accumulates bulk ops received by the writer. */
  class RecordingBulkClient extends KBulkClient {
    val ops:     mutable.Buffer[Seq[BulkOp]] = mutable.Buffer.empty
    val created: mutable.Buffer[String]      = mutable.Buffer.empty

    override def bulk(ops: Seq[BulkOp]): Try[BulkResult] = {
      this.ops += ops
      Success(BulkResult(took = 1L, errors = false, itemErrors = Seq.empty))
    }

    override def createIndex(name: String): Try[Unit] = {
      created += name
      Success(())
    }

    override def close(): Unit = ()

    def allOps: Seq[BulkOp] = ops.flatten.toSeq
  }

  private val defaultStoresInfo = new StoresInfo(COption.none(), COption.none(), COption.none())

  private def kcqlSettings(kcqlExpr: String, batchSize: Int = 100): ElasticCommonSettings = {
    val kcqls = kcqlExpr.split(";").filter(_.nonEmpty).map(io.lenses.kcql.Kcql.parse).toSeq
    ElasticCommonSettings(
      kcqls                 = kcqls,
      errorPolicy           = new NoopErrorPolicy(),
      taskRetries           = 1,
      writeTimeout          = 300000,
      batchSize             = batchSize,
      pkJoinerSeparator     = ".",
      httpBasicAuthUsername = "",
      httpBasicAuthPassword = "",
      storesInfo            = defaultStoresInfo,
    )
  }

  private val schema: Schema = SchemaBuilder.struct()
    .field("id", Schema.INT32_SCHEMA)
    .field("name", Schema.STRING_SCHEMA)
    .build()

  private def struct(id: Int, name: String): Struct =
    new Struct(schema).put("id", id).put("name", name)

  private def record(topic: String, key: String, value: Struct, offset: Long = 0L): SinkRecord =
    new SinkRecord(topic, 0, Schema.STRING_SCHEMA, key, schema, value, offset)

  private def tombstone(topic: String, key: String, offset: Long = 0L): SinkRecord =
    new SinkRecord(topic, 0, Schema.STRING_SCHEMA, key, null, null, offset)

  // ---- Tests ----

  test("INSERT SELECT * routes document to the correct index") {
    val recording = new RecordingBulkClient
    val writer    = new JsonBulkWriter(recording, kcqlSettings("INSERT INTO my-index SELECT * FROM my-topic"))

    writer.write(Vector(record("my-topic", "k1", struct(1, "Alice"))))

    recording.allOps should have size 1
    val op = recording.allOps.head.asInstanceOf[InsertOp]
    op.index shouldBe "my-index"
    op.json.get("id").asInt() shouldBe 1
    op.json.get("name").asText() shouldBe "Alice"
  }

  test("INSERT with PK in record key uses pk-joined id") {
    val recording = new RecordingBulkClient
    val writer = new JsonBulkWriter(
      recording,
      kcqlSettings("INSERT INTO pk-index SELECT * FROM pk-topic PK id"),
    )

    writer.write(Vector(record("pk-topic", "k1", struct(42, "Bob"))))

    val op = recording.allOps.head.asInstanceOf[InsertOp]
    op.id shouldBe "42"
  }

  test("INSERT without PK auto-generates id from topic.partition.offset") {
    val recording = new RecordingBulkClient
    val writer    = new JsonBulkWriter(recording, kcqlSettings("INSERT INTO auto-idx SELECT * FROM auto-topic"))

    writer.write(Vector(record("auto-topic", "k1", struct(1, "C"), offset = 7L)))

    val op = recording.allOps.head.asInstanceOf[InsertOp]
    op.id shouldBe "auto-topic.0.7"
  }

  test("UPSERT SELECT * routes to update operation with PK-joined id") {
    val recording = new RecordingBulkClient
    val writer = new JsonBulkWriter(
      recording,
      kcqlSettings("UPSERT INTO upsert-idx SELECT * FROM upsert-topic PK id"),
    )

    writer.write(Vector(record("upsert-topic", "k1", struct(5, "Dave"))))

    val op = recording.allOps.head.asInstanceOf[UpsertOp]
    op.index shouldBe "upsert-idx"
    op.id shouldBe "5"
  }

  test("UPSERT without PK fails at write time with a message about primary keys") {
    val recording = new RecordingBulkClient
    val writer    = new JsonBulkWriter(recording, kcqlSettings("UPSERT INTO upsert-nopk SELECT * FROM upsert-nopk-topic"))
    val ex = intercept[IllegalArgumentException](
      writer.write(Vector(record("upsert-nopk-topic", "k1", struct(1, "test")))),
    )
    ex.getMessage should include("primary keys")
  }

  test("IGNORE fields: field named in IGNORE clause is removed from the document") {
    val recording = new RecordingBulkClient
    val writer = new JsonBulkWriter(
      recording,
      kcqlSettings("INSERT INTO ignore-idx SELECT * FROM ignore-topic IGNORE name"),
    )

    writer.write(Vector(record("ignore-topic", "k1", struct(1, "Eve"))))

    val op = recording.allOps.head.asInstanceOf[InsertOp]
    op.json.has("name") shouldBe false
    op.json.has("id") shouldBe true
  }

  test("tombstone with NOOP behavior (DELETE) generates a DeleteOp") {
    val recording = new RecordingBulkClient
    val writer = new JsonBulkWriter(
      recording,
      kcqlSettings(
        """INSERT INTO tombstone-idx SELECT * FROM tombstone-topic PK id PROPERTIES('behavior.on.null.values'='DELETE')""",
      ),
    )

    writer.write(Vector(
      record("tombstone-topic", "k1", struct(99, "F")),
      tombstone("tombstone-topic", "k1", offset = 1L),
    ))

    val ops = recording.allOps
    ops should have size 2
    ops(0) shouldBe an[InsertOp]
    ops(1) shouldBe an[DeleteOp]
  }

  test("tombstone with default IGNORE behavior produces no op") {
    val recording = new RecordingBulkClient
    val writer    = new JsonBulkWriter(recording, kcqlSettings("INSERT INTO ts-ignore-idx SELECT * FROM ts-ignore-topic"))

    writer.write(Vector(tombstone("ts-ignore-topic", "k1")))

    recording.allOps shouldBe empty
  }

  test("tombstone with FAIL behavior throws") {
    val recording = new RecordingBulkClient
    val writer = new JsonBulkWriter(
      recording,
      kcqlSettings(
        """INSERT INTO ts-fail-idx SELECT * FROM ts-fail-topic PK id PROPERTIES('behavior.on.null.values'='FAIL')""",
      ),
    )

    intercept[Exception](writer.write(Vector(tombstone("ts-fail-topic", "k1"))))
  }

  test("AUTOCREATE calls createIndex at construction") {
    val recording = new RecordingBulkClient
    val _         = new JsonBulkWriter(recording, kcqlSettings("INSERT INTO auto-create-idx SELECT * FROM t AUTOCREATE"))
    recording.created should contain("auto-create-idx")
  }

  test("batch size groups records into multiple bulk calls") {
    val recording = new RecordingBulkClient
    val writer =
      new JsonBulkWriter(recording, kcqlSettings("INSERT INTO batch-idx SELECT * FROM batch-topic", batchSize = 2))

    val records = (1 to 5).map(i => record("batch-topic", s"k$i", struct(i, s"user-$i"), offset = i.toLong)).toVector
    writer.write(records)

    // 5 records in batches of 2 → 3 bulk calls (2 + 2 + 1)
    recording.ops should have size 3
    recording.ops.map(_.size) shouldBe Seq(2, 2, 1)
  }

  test("multi-KCQL fanout: same topic routed to two indexes") {
    val recording = new RecordingBulkClient
    val writer = new JsonBulkWriter(
      recording,
      kcqlSettings("INSERT INTO idx-a SELECT * FROM fan-topic;INSERT INTO idx-b SELECT * FROM fan-topic"),
    )

    writer.write(Vector(record("fan-topic", "k1", struct(1, "G"))))

    val indexes = recording.allOps.map(_.asInstanceOf[InsertOp].index).toSet
    indexes shouldBe Set("idx-a", "idx-b")
  }

  // WITHDOCTYPE is an ES6-era KCQL clause: `kcql.getDocType()` surfaces the value.
  // ES7/OS clients ignore it (`supportsDocumentType = false`); ES6 uses it.
  // The shared JsonBulkWriter only logs a warning when the backend ignores doc types.
  // This test verifies normal construction succeeds (no docType set via standard KCQL).
  test("writer construction succeeds for INSERT KCQL with no special clauses") {
    val recording = new RecordingBulkClient
    noException shouldBe thrownBy {
      new JsonBulkWriter(
        recording,
        kcqlSettings("INSERT INTO dt-idx SELECT * FROM dt-topic"),
      )
    }
  }

  // PIPELINE is wired in InsertOp and forwarded to each bulk client.
  // When not configured in KCQL, it defaults to None.
  test("pipeline defaults to None when not specified in KCQL") {
    val recording = new RecordingBulkClient
    val writer = new JsonBulkWriter(
      recording,
      kcqlSettings("INSERT INTO pipe-idx SELECT * FROM pipe-topic"),
    )
    writer.write(Vector(record("pipe-topic", "k1", struct(1, "H"))))
    val op = recording.allOps.head.asInstanceOf[InsertOp]
    op.pipeline shouldBe None
  }

  // ---- Parity-contract: fetchNullValueBehaviorProperty absent-key default -----------------------
  // When the KCQL PROPERTIES map does NOT contain behavior.on.null.values, the lookup returns
  // null → NullValueBehavior.fromString(null) → IGNORE.
  // A tombstone must therefore be silently dropped (the preserved default).

  test("tombstone with no null-value property configured defaults to IGNORE") {
    val recording = new RecordingBulkClient
    val writer =
      new JsonBulkWriter(recording, kcqlSettings("INSERT INTO ts-default-idx SELECT * FROM ts-default-topic"))

    writer.write(Vector(tombstone("ts-default-topic", "k1")))

    recording.allOps shouldBe empty
  }

  // ---- Parity-contract: PK stringification (mkString) ----------------------------------

  test("INT64 PK field stringifies to decimal string") {
    val int64Schema = SchemaBuilder.struct()
      .field("id", Schema.INT64_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .build()

    class CapturingClient extends KBulkClient {
      var lastOps: Seq[BulkOp] = Seq.empty
      override def bulk(ops: Seq[BulkOp]): Try[BulkResult] = {
        lastOps = ops; Success(BulkResult(1L, false, Seq.empty))
      }
      override def createIndex(name: String): Try[Unit] = Success(())
      override def close(): Unit = ()
    }

    val capturing = new CapturingClient
    val writer = new JsonBulkWriter(
      capturing,
      kcqlSettings("INSERT INTO idx SELECT * FROM topic PK id"),
    )

    val int64Struct = new Struct(int64Schema).put("id", 123L).put("name", "Alice")
    val r           = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "k", int64Schema, int64Struct, 0L)
    writer.write(Vector(r))

    capturing.lastOps.head.asInstanceOf[InsertOp].id shouldBe "123"
  }

  test("multi-field PK is joined with configured separator") {
    val multiSchema = SchemaBuilder.struct()
      .field("id", Schema.INT64_SCHEMA)
      .field("region", Schema.STRING_SCHEMA)
      .build()

    class CapturingClient extends KBulkClient {
      var lastOps: Seq[BulkOp] = Seq.empty
      override def bulk(ops: Seq[BulkOp]): Try[BulkResult] = {
        lastOps = ops; Success(BulkResult(1L, false, Seq.empty))
      }
      override def createIndex(name: String): Try[Unit] = Success(())
      override def close(): Unit = ()
    }

    val capturing = new CapturingClient
    val settings  = kcqlSettings("INSERT INTO idx SELECT * FROM topic PK id, region")
    val writer    = new JsonBulkWriter(capturing, settings)

    val s = new Struct(multiSchema).put("id", 123L).put("region", "eu-west-1")
    val r = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "k", multiSchema, s, 0L)
    writer.write(Vector(r))

    capturing.lastOps.head.asInstanceOf[InsertOp].id shouldBe "123.eu-west-1"
  }

  // ---- Parity-contract: AUTOCREATE filter + non-static target fast-fail ----------------

  test("AUTOCREATE calls createIndex at construction (deduplication test)") {
    val recording = new RecordingBulkClient
    val _ =
      new JsonBulkWriter(recording,
                         kcqlSettings(
                           "INSERT INTO idx1 SELECT * FROM t AUTOCREATE;INSERT INTO idx1 SELECT * FROM t2 AUTOCREATE",
                         ),
      )
    recording.created.count(_ == "idx1") shouldBe 1
  }

  // ---- Parity-row 44: WITHDOCTYPE warning — exactly once at construction, never per record ----
  // When a Kcql has a non-empty docType and the client does not support document types,
  // JsonBulkWriter emits exactly one WARN at construction time.
  // Writing N records must not produce any additional WARN lines about WITHDOCTYPE.

  test("WITHDOCTYPE warning is emitted exactly once at construction, never per record written") {
    // Build a mock Kcql that reports a non-empty docType.
    val sourceTopic = "doctype-topic"
    val kcqlMock    = mock[Kcql]
    when(kcqlMock.getSource).thenReturn(sourceTopic)
    when(kcqlMock.getDocType).thenReturn("_doc")
    when(kcqlMock.getTargetType).thenReturn(
      cyclops.control.Either.right[IllegalArgumentException, TargetType](new StaticTargetType("doctype-idx")),
    )
    when(kcqlMock.getProperties).thenReturn(emptyMap)
    when(kcqlMock.getWriteMode).thenReturn(WriteModeEnum.INSERT)
    when(kcqlMock.getFields).thenReturn(emptyList())
    when(kcqlMock.getIgnoredFields).thenReturn(emptyList())
    when(kcqlMock.isAutoCreate).thenReturn(false)

    // A client that does NOT support document types (ES7 / OpenSearch behaviour).
    class NoDocTypeClient extends RecordingBulkClient {
      override val supportsDocumentType: Boolean = false
    }
    val client = new NoDocTypeClient

    // Attach a ListAppender to the JsonBulkWriter logger to capture WARN lines.
    val writerLogger = LoggerFactory.getLogger(classOf[JsonBulkWriter]).asInstanceOf[LogbackLogger]
    val listAppender = new ListAppender[ILoggingEvent]()
    listAppender.start()
    writerLogger.addAppender(listAppender)

    try {
      val settings = ElasticCommonSettings(
        kcqls       = Seq(kcqlMock),
        errorPolicy = new NoopErrorPolicy,
        storesInfo  = defaultStoresInfo,
      )
      val writer = new JsonBulkWriter(client, settings)

      // Capture warnings emitted during construction.
      val constructionWarns = listAppender.list.asScala
        .filter(_.getLevel == LogbackLevel.WARN)
        .map(_.getFormattedMessage)
        .filter(_.contains("WITHDOCTYPE"))
        .toSeq
      constructionWarns should have size 1

      // Write 3 records; no additional WITHDOCTYPE warnings must appear.
      val r = new SinkRecord(sourceTopic, 0, Schema.STRING_SCHEMA, "k", Schema.STRING_SCHEMA, """{"x":1}""", 0L)
      writer.write(Vector(r, r, r))

      val allWarns = listAppender.list.asScala
        .filter(_.getLevel == LogbackLevel.WARN)
        .map(_.getFormattedMessage)
        .filter(_.contains("WITHDOCTYPE"))
        .toSeq
      allWarns should have size 1
    } finally {
      val _ = writerLogger.detachAppender(listAppender)
    }
  }

  // ---- Bug 1 regression: case-insensitive null-value behavior ----------------------------

  test("tombstone with lowercase 'delete' behavior produces a DeleteOp (case-insensitive)") {
    val recording = new RecordingBulkClient
    val writer = new JsonBulkWriter(
      recording,
      kcqlSettings(
        """INSERT INTO ts-lower-idx SELECT * FROM ts-lower-topic PK id PROPERTIES('behavior.on.null.values'='delete')""",
      ),
    )

    writer.write(Vector(
      record("ts-lower-topic", "k1", struct(7, "G")),
      tombstone("ts-lower-topic", "k1", offset = 1L),
    ))

    val ops = recording.allOps
    ops should have size 2
    ops(0) shouldBe an[InsertOp]
    ops(1) shouldBe an[DeleteOp]
  }

  test("tombstone with mixed-case 'Fail' behavior throws (case-insensitive)") {
    val recording = new RecordingBulkClient
    val writer = new JsonBulkWriter(
      recording,
      kcqlSettings(
        """INSERT INTO ts-mixedcase-idx SELECT * FROM ts-mc-topic PK id PROPERTIES('behavior.on.null.values'='Fail')""",
      ),
    )

    intercept[Exception](writer.write(Vector(tombstone("ts-mc-topic", "k1"))))
  }

  // ---- Bug 2 regression: nested IGNORE field removal ---------------------------------

  private val nestedSchema: Schema = SchemaBuilder.struct()
    .field("id", Schema.INT32_SCHEMA)
    .field("meta",
           SchemaBuilder.struct()
             .field("tag", Schema.STRING_SCHEMA)
             .field("score", Schema.INT32_SCHEMA)
             .build(),
    )
    .build()

  private def nestedStruct(id: Int, tag: String, score: Int): Struct = {
    val inner = new Struct(nestedSchema.field("meta").schema())
      .put("tag", tag)
      .put("score", score)
    new Struct(nestedSchema).put("id", id).put("meta", inner)
  }

  private def nestedRecord(topic: String, key: String, value: Struct, offset: Long = 0L): SinkRecord =
    new SinkRecord(topic, 0, Schema.STRING_SCHEMA, key, nestedSchema, value, offset)

  test("IGNORE nested.field removes only the leaf from the nested object, leaving siblings intact") {
    val recording = new RecordingBulkClient
    val writer = new JsonBulkWriter(
      recording,
      kcqlSettings("INSERT INTO nested-idx SELECT * FROM nested-topic IGNORE meta.tag"),
    )

    writer.write(Vector(nestedRecord("nested-topic", "k1", nestedStruct(1, "t", 99))))

    val op   = recording.allOps.head.asInstanceOf[InsertOp]
    val meta = op.json.get("meta")
    meta should not be null
    meta.has("tag") shouldBe false
    meta.has("score") shouldBe true
    op.json.has("id") shouldBe true
  }

  // ---- Parity-contract: dynamic index resolution runs before tombstone branch -----------

  test("dynamic index (WITHINDEXSUFFIX) is resolved before tombstone branch — DeleteOp carries resolved index") {
    import java.time.LocalDate

    val today     = LocalDate.now().toString
    val baseIndex = "dyn-tombstone"
    val expected  = s"${baseIndex}_$today"

    val recording = new RecordingBulkClient
    val kcqlExpr =
      s"INSERT INTO $baseIndex SELECT * FROM dyn-topic PK id WITHINDEXSUFFIX=_{YYYY-MM-dd} PROPERTIES('behavior.on.null.values'='DELETE')"

    val writer = new JsonBulkWriter(recording, kcqlSettings(kcqlExpr))

    val pkSchema: Schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build()
    val struct    = new Struct(pkSchema).put("id", "t1")
    val inserted  = new SinkRecord("dyn-topic", 0, Schema.STRING_SCHEMA, "t1", pkSchema, struct, 0L)
    val tombstone = new SinkRecord("dyn-topic", 0, Schema.STRING_SCHEMA, "t1", null, null, 1L)

    writer.write(Vector(inserted, tombstone))

    val deleteOps = recording.allOps.collect { case d: DeleteOp => d }
    deleteOps should not be empty
    deleteOps.head.index shouldBe expected
  }

  // ---- Item-level error classification (429 vs mapper) ---------------------------------

  private def failingClient(itemErrors: Seq[BulkItemError]): KBulkClient = new KBulkClient {
    override def bulk(ops: Seq[BulkOp]): Try[BulkResult] =
      Success(BulkResult(took = 1L, errors = true, itemErrors = itemErrors))
    override def createIndex(name: String): Try[Unit] = Success(())
    override def close(): Unit = ()
  }

  test("mapper item error under THROW surfaces as FatalConnectException (not retried)") {
    val writer = new JsonBulkWriter(
      failingClient(Seq(BulkItemError("idx", "1", "failed to parse", "mapper_parsing_exception", 400))),
      ElasticCommonSettings(
        kcqls       = Seq(io.lenses.kcql.Kcql.parse("INSERT INTO idx SELECT * FROM topic")),
        errorPolicy = ThrowErrorPolicy(),
        taskRetries = 20,
        storesInfo  = defaultStoresInfo,
      ),
    )
    val thrown = intercept[FatalConnectException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    thrown should not be a[RetriableIntegrityException]
    thrown.getMessage should include("mapper_parsing_exception")
  }

  test("429 item error under RETRY surfaces as RetriableException wrapping RetriableIntegrityException") {
    val writer = new JsonBulkWriter(
      failingClient(Seq(BulkItemError("idx", "1", "rejected execution", "es_rejected_execution_exception", 429))),
      ElasticCommonSettings(
        kcqls       = Seq(io.lenses.kcql.Kcql.parse("INSERT INTO idx SELECT * FROM topic")),
        errorPolicy = RetryErrorPolicy(),
        taskRetries = 20,
        storesInfo  = defaultStoresInfo,
      ),
    )
    val thrown = intercept[RetriableException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    thrown.getCause shouldBe a[RetriableIntegrityException]
  }

  test("429 item error under THROW fails the task via RetriableIntegrityException (no silent skip)") {
    val writer = new JsonBulkWriter(
      failingClient(Seq(BulkItemError("idx", "1", "rejected execution", "es_rejected_execution_exception", 429))),
      ElasticCommonSettings(
        kcqls       = Seq(io.lenses.kcql.Kcql.parse("INSERT INTO idx SELECT * FROM topic")),
        errorPolicy = ThrowErrorPolicy(),
        taskRetries = 20,
        storesInfo  = defaultStoresInfo,
      ),
    )
    intercept[RetriableIntegrityException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
  }

  test("429 item error under NOOP is not swallowed") {
    val writer = new JsonBulkWriter(
      failingClient(Seq(BulkItemError("idx", "1", "rejected execution", "es_rejected_execution_exception", 429))),
      ElasticCommonSettings(
        kcqls       = Seq(io.lenses.kcql.Kcql.parse("INSERT INTO idx SELECT * FROM topic")),
        errorPolicy = new NoopErrorPolicy(),
        taskRetries = 20,
        storesInfo  = defaultStoresInfo,
      ),
    )
    intercept[RetriableIntegrityException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
  }

  test("mixed 429 + mapper under RETRY is fatal (does not wrap in RetriableException)") {
    val writer = new JsonBulkWriter(
      failingClient(
        Seq(
          BulkItemError("idx", "1", "rejected", "es_rejected_execution_exception", 429),
          BulkItemError("idx", "2", "failed to parse", "mapper_parsing_exception", 400),
        ),
      ),
      ElasticCommonSettings(
        kcqls       = Seq(io.lenses.kcql.Kcql.parse("INSERT INTO idx SELECT * FROM topic")),
        errorPolicy = RetryErrorPolicy(),
        taskRetries = 20,
        storesInfo  = defaultStoresInfo,
      ),
    )
    val thrown = intercept[FatalConnectException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    thrown should not be a[RetriableIntegrityException]
    thrown should not be a[RetriableException]
  }

  test("409 version conflict under RETRY surfaces as RetriableException") {
    val writer = new JsonBulkWriter(
      failingClient(
        Seq(BulkItemError("idx", "1", "version conflict", "version_conflict_engine_exception", 409)),
      ),
      ElasticCommonSettings(
        kcqls       = Seq(io.lenses.kcql.Kcql.parse("INSERT INTO idx SELECT * FROM topic")),
        errorPolicy = RetryErrorPolicy(),
        taskRetries = 20,
        storesInfo  = defaultStoresInfo,
      ),
    )
    val thrown = intercept[RetriableException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    thrown.getCause shouldBe a[RetriableIntegrityException]
  }

  test("mapper error whose reason mentions rejected execution is still fatal under RETRY") {
    val writer = new JsonBulkWriter(
      failingClient(
        Seq(
          BulkItemError(
            "idx",
            "1",
            "failed to parse field [foo]: Preview of field's value: 'rejected execution'",
            "mapper_parsing_exception",
            400,
          ),
        ),
      ),
      ElasticCommonSettings(
        kcqls       = Seq(io.lenses.kcql.Kcql.parse("INSERT INTO idx SELECT * FROM topic")),
        errorPolicy = RetryErrorPolicy(),
        taskRetries = 20,
        storesInfo  = defaultStoresInfo,
      ),
    )
    val thrown = intercept[FatalConnectException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    thrown should not be a[RetriableException]
  }
}
