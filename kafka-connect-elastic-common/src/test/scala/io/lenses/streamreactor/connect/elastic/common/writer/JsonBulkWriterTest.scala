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
import io.lenses.streamreactor.connect.elastic.common.config.ItemErrorMode
import io.lenses.streamreactor.connect.elastic.common.dlq.DlqGate
import io.lenses.streamreactor.connect.elastic.common.dlq.RecordRef
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
import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.mockito.ArgumentMatchersSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.util.Collections.emptyList
import java.util.Collections.emptyMap
import java.util.concurrent.CompletableFuture
import scala.collection.mutable
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Failure
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

  /** Records every (record, cause) it was asked to report and always acknowledges immediately. */
  class FakeReporter extends ErrantRecordReporter {
    val received: mutable.Buffer[(SinkRecord, Throwable)] = mutable.Buffer.empty
    override def report(record: SinkRecord, error: Throwable): java.util.concurrent.Future[Void] = {
      received += (record -> error)
      CompletableFuture.completedFuture[Void](null)
    }
  }

  private def dlqGate(reporter: ErrantRecordReporter, dlqTopicConfigured: Boolean = true): DlqGate =
    DlqGate(Some(reporter), dlqTopicConfigured)

  /**
   * A client that fails specific ops (by id) with a caller-supplied [[BulkItemError]], computing
   * the correct `position`/`index`/`id` for each response item the same way a real client would.
   * Ops whose id is not in `errorsByOpId` succeed.
   */
  class ScriptedBulkClient(errorsByOpId: Map[String, BulkItemError]) extends KBulkClient {
    val calls: mutable.Buffer[Seq[BulkOp]] = mutable.Buffer.empty

    override def bulk(ops: Seq[BulkOp]): Try[BulkResult] = {
      calls += ops
      val itemErrors = ops.zipWithIndex.flatMap {
        case (op, idx) => errorsByOpId.get(op.id).map(_.copy(index = op.index, id = op.id, position = idx))
      }
      Success(BulkResult(took = 1L, errors = itemErrors.nonEmpty, itemErrors = itemErrors))
    }
    override def createIndex(name: String): Try[Unit] = Success(())
    override def close(): Unit = ()
  }

  private def mapperError(reason: String = "failed to parse"): BulkItemError =
    BulkItemError(index = "", id = "", reason = reason, errorType = "mapper_parsing_exception", status = 400)

  private def rejectedError(): BulkItemError =
    BulkItemError(index = "", id = "", reason = "rejected", errorType = "es_rejected_execution_exception", status = 429)

  private def settingsWith(
    kcqlExpr:  String,
    mode:      ItemErrorMode,
    policy:    io.lenses.streamreactor.common.errors.ErrorPolicy = ThrowErrorPolicy(),
    batchSize: Int                                               = 100,
  ): ElasticCommonSettings = {
    val kcqls = kcqlExpr.split(";").filter(_.nonEmpty).map(io.lenses.kcql.Kcql.parse).toSeq
    ElasticCommonSettings(
      kcqls         = kcqls,
      errorPolicy   = policy,
      taskRetries   = 20,
      batchSize     = batchSize,
      storesInfo    = defaultStoresInfo,
      itemErrorMode = mode,
    )
  }

  test("dlq mode with a DLQ available reports a permanent item error and does not throw; offsets can advance") {
    val reporter = new FakeReporter
    val client   = new ScriptedBulkClient(Map("1" -> mapperError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Dlq),
      dlqGate(reporter),
    )

    val r        = record("topic", "k", struct(1, "x"))
    val reported = writer.write(Vector(r))

    reported shouldBe Set(RecordRef.of(r))
    reporter.received should have size 1
    reporter.received.head._1 shouldBe r
  }

  test("fail mode (the default) still throws even when a DLQ is available — skip must be opted into") {
    val reporter = new FakeReporter
    val client   = new ScriptedBulkClient(Map("1" -> mapperError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Fail),
      dlqGate(reporter),
    )

    intercept[FatalConnectException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    reporter.received shouldBe empty
  }

  test("dlq mode without a configured DLQ topic falls back to throwing (reporter alone is not enough)") {
    val reporter = new FakeReporter
    val client   = new ScriptedBulkClient(Map("1" -> mapperError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Dlq),
      dlqGate(reporter, dlqTopicConfigured = false),
    )

    intercept[FatalConnectException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    reporter.received shouldBe empty
  }

  test("dlq mode with no reporter at all (context never handed one out) falls back to throwing") {
    val client = new ScriptedBulkClient(Map("1" -> mapperError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Dlq),
      DlqGate.disabled,
    )

    intercept[FatalConnectException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
  }

  test(
    "dlq mode: a mixed batch (permanent + retriable in the SAME response) diverts NOTHING — diversion is deferred until a clean attempt",
  ) {
    val reporter = new FakeReporter
    val client = new ScriptedBulkClient(
      Map("1" -> rejectedError(), "2" -> mapperError()),
    )
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Dlq),
      dlqGate(reporter),
    )

    val poison = record("topic", "k2", struct(2, "bad"), offset = 1L)
    val ok     = record("topic", "k1", struct(1, "transient"), offset = 0L)

    // Diverting the mapper conflict here, then throwing for the retriable remainder, would mean
    // it gets diverted AGAIN on every redelivery while the 429 persists — unbounded
    // amplification. So a retriable remainder in the SAME response defers diversion entirely:
    // the full error set throws, and nothing is reported.
    val thrown = intercept[FatalConnectException](writer.write(Vector(ok, poison)))
    thrown.getMessage should include("es_rejected_execution_exception")
    thrown.getMessage should include("mapper_parsing_exception")
    reporter.received shouldBe empty
  }

  test("dlq mode at batch size N: exactly the poison record is reported, its batch-mates are written normally") {
    val reporter = new FakeReporter
    // Records 1..5 in one batch of 5; only id "3" is poison.
    val client = new ScriptedBulkClient(Map("3" -> mapperError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Dlq, batchSize = 5),
      dlqGate(reporter),
    )

    val records  = (1 to 5).map(i => record("topic", s"k$i", struct(i, s"v$i"), offset = i.toLong)).toVector
    val reported = writer.write(records)

    reported shouldBe Set(RecordRef.of(records(2))) // id "3" is the 3rd record (index 2)
    reporter.received.map(_._1) shouldBe Seq(records(2))
    client.calls should have size 1
    client.calls.head should have size 5 // the whole batch of 5 still went to the client in one bulk() call
  }

  test("dlq mode: a writer-stage failure (FAIL tombstone) is reported to the DLQ instead of failing the task") {
    val reporter = new FakeReporter
    val client   = new ScriptedBulkClient(Map.empty)
    val writer = new JsonBulkWriter(
      client,
      settingsWith(
        "INSERT INTO idx SELECT * FROM topic PK id PROPERTIES('behavior.on.null.values'='FAIL')",
        ItemErrorMode.Dlq,
      ),
      dlqGate(reporter),
    )

    val ts       = tombstone("topic", "k1", offset = 3L)
    val reported = writer.write(Vector(ts))

    reported shouldBe Set(RecordRef.of(ts))
    reporter.received.map(_._1) shouldBe Seq(ts)
    client.calls shouldBe empty // the tombstone never became a BulkOp at all
  }

  test("fail mode: a writer-stage failure (FAIL tombstone) still throws, DLQ or not") {
    val reporter = new FakeReporter
    val client   = new ScriptedBulkClient(Map.empty)
    val writer = new JsonBulkWriter(
      client,
      settingsWith(
        "INSERT INTO idx SELECT * FROM topic PK id PROPERTIES('behavior.on.null.values'='FAIL')",
        ItemErrorMode.Fail,
      ),
      dlqGate(reporter),
    )

    intercept[IllegalStateException](writer.write(Vector(tombstone("topic", "k1"))))
    reporter.received shouldBe empty
  }

  test(
    "a transport-level failure is reclassified as RetriableIntegrityException and is never reported to the DLQ, even in dlq mode",
  ) {
    val reporter = new FakeReporter
    val client = new KBulkClient {
      override def bulk(ops:         Seq[BulkOp]): Try[BulkResult] = Failure(new RuntimeException("connection reset"))
      override def createIndex(name: String):      Try[Unit]       = Success(())
      override def close(): Unit = ()
    }
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic", ItemErrorMode.Dlq, policy = NoopErrorPolicy()),
      dlqGate(reporter),
    )

    // Transport failures are reclassified as RetriableIntegrityException regardless of DLQ mode.
    val thrown = intercept[RetriableIntegrityException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    thrown.getMessage should include("connection reset")
    reporter.received shouldBe empty
  }

  test("a transport-level failure under RETRY is still retried while budget remains (not fatal)") {
    val client = new KBulkClient {
      override def bulk(ops:         Seq[BulkOp]): Try[BulkResult] = Failure(new RuntimeException("timeout"))
      override def createIndex(name: String):      Try[Unit]       = Success(())
      override def close(): Unit = ()
    }
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic", ItemErrorMode.Fail, policy = RetryErrorPolicy()),
    )

    val thrown = intercept[RetriableException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    thrown.getCause shouldBe a[RetriableIntegrityException]
  }

  test("dlq mode: multi-KCQL fanout reports the same record independently for each target it fails at") {
    val reporter = new FakeReporter
    val client   = new ScriptedBulkClient(Map("1" -> mapperError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith(
        "INSERT INTO idx-a SELECT * FROM fan-topic PK id;INSERT INTO idx-b SELECT * FROM fan-topic PK id",
        ItemErrorMode.Dlq,
      ),
      dlqGate(reporter),
    )

    val r        = record("fan-topic", "k1", struct(1, "x"))
    val reported = writer.write(Vector(r))

    // The same SinkRecord fails at BOTH idx-a and idx-b (same op id "1" in each KCQL's batch) —
    // both are genuine, independent failures and both must be reported, not deduplicated away.
    // (Diverted at both idx-a and idx-b, but RecordRef has no `target` field, so the SET
    // collapses to size 1 — the assertion below on reporter.received is what proves both fired.)
    reported shouldBe Set(RecordRef.of(r))
    reporter.received should have size 2
    reporter.received.foreach(_._1 shouldBe r)
  }

  private def clusterBlockError(): BulkItemError =
    BulkItemError(index = "", id = "", reason = "index blocked", errorType = "cluster_block_exception", status = 403)

  private def indexNotFoundError(): BulkItemError =
    BulkItemError(index = "", id = "", reason = "no such index", errorType = "index_not_found_exception", status = 404)

  private def indexClosedError(): BulkItemError =
    BulkItemError(index = "", id = "", reason = "closed", errorType = "index_closed_exception", status = 400)

  private def missingPipelineError(): BulkItemError =
    BulkItemError(
      index     = "",
      id        = "",
      reason    = "pipeline with id [my-pipeline] does not exist",
      errorType = "illegal_argument_exception",
      status    = 400,
    )

  test("dlq mode: a 403 cluster_block_exception is never diverted to the DLQ — the batch throws instead") {
    val reporter = new FakeReporter
    val client   = new ScriptedBulkClient(Map("1" -> clusterBlockError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Dlq),
      dlqGate(reporter),
    )

    val thrown = intercept[FatalConnectException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    thrown.getMessage should include("cluster_block_exception")
    reporter.received shouldBe empty
  }

  test("dlq mode: a 404 index_not_found_exception is never diverted to the DLQ — the batch throws instead") {
    val reporter = new FakeReporter
    val client   = new ScriptedBulkClient(Map("1" -> indexNotFoundError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Dlq),
      dlqGate(reporter),
    )

    val thrown = intercept[FatalConnectException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    thrown.getMessage should include("index_not_found_exception")
    reporter.received shouldBe empty
  }

  test(
    "dlq mode: a batch mixing a 400 mapper error with a 403 cluster_block_exception diverts NOTHING — a batch-wide condition blocks the whole call",
  ) {
    val reporter = new FakeReporter
    val client = new ScriptedBulkClient(
      Map("1" -> mapperError(), "2" -> clusterBlockError()),
    )
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Dlq),
      dlqGate(reporter),
    )

    val good = record("topic", "k1", struct(1, "good"), offset = 0L)
    val bad  = record("topic", "k2", struct(2, "bad"), offset = 1L)

    // Even though "1" is a perfectly good DLQ candidate on its own, the presence of the 403 in
    // the SAME response means the whole call is untrustworthy — see
    // BulkItemErrorClassifier.isDivertible and JsonBulkWriter.handleBulkResult.
    intercept[FatalConnectException](writer.write(Vector(good, bad)))
    reporter.received shouldBe empty
  }

  test("dlq mode: an all-400 index_closed_exception batch diverts NOTHING and throws (batch-wide)") {
    val reporter = new FakeReporter
    val client   = new ScriptedBulkClient(Map("1" -> indexClosedError(), "2" -> indexClosedError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Dlq),
      dlqGate(reporter),
    )

    val thrown = intercept[FatalConnectException](
      writer.write(Vector(record("topic", "k1", struct(1, "a")), record("topic", "k2", struct(2, "b"), offset = 1L))),
    )
    thrown.getMessage should include("index_closed_exception")
    reporter.received shouldBe empty
  }

  test("drop mode: an all-400 index_closed_exception batch drops NOTHING and throws (no silent data loss)") {
    val client = new ScriptedBulkClient(Map("1" -> indexClosedError(), "2" -> indexClosedError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Drop),
    )

    val reported = intercept[FatalConnectException](
      writer.write(Vector(record("topic", "k1", struct(1, "a")), record("topic", "k2", struct(2, "b"), offset = 1L))),
    )
    reported.getMessage should include("index_closed_exception")
  }

  test("drop mode: an all-400 missing-ingest-pipeline batch drops NOTHING and throws (batch-wide)") {
    val client = new ScriptedBulkClient(Map("1" -> missingPipelineError(), "2" -> missingPipelineError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Drop),
    )

    val thrown = intercept[FatalConnectException](
      writer.write(Vector(record("topic", "k1", struct(1, "a")), record("topic", "k2", struct(2, "b"), offset = 1L))),
    )
    thrown.getMessage should include("pipeline with id")
  }

  test("dlq mode: a batch mixing a genuine 400 mapper error with a batch-wide 400 (closed index) diverts NOTHING") {
    val reporter = new FakeReporter
    val client   = new ScriptedBulkClient(Map("1" -> mapperError(), "2" -> indexClosedError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Dlq),
      dlqGate(reporter),
    )

    intercept[FatalConnectException](
      writer.write(Vector(record("topic", "k1", struct(1, "a")), record("topic", "k2", struct(2, "b"), offset = 1L))),
    )
    reporter.received shouldBe empty
  }

  test(
    "dlq mode: redelivery of a PERSISTENTLY mixed batch never diverts anything, on any attempt",
  ) {
    val reporter = new FakeReporter
    // Simulates the SAME batch being redelivered by Connect after the retriable remainder
    // fails every previous attempt: id "2" is permanently poison, id "1" is retriable every time.
    val client = new ScriptedBulkClient(Map("1" -> rejectedError(), "2" -> mapperError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Dlq),
      dlqGate(reporter),
    )

    val poison = record("topic", "k2", struct(2, "bad"), offset = 1L)
    val ok     = record("topic", "k1", struct(1, "transient"), offset = 0L)

    // Attempt 1: mixed response -> diversion deferred, nothing reported, full set thrown.
    intercept[FatalConnectException](writer.write(Vector(ok, poison)))
    // Attempt 2: Connect redelivers the identical batch; the retriable condition still hasn't
    // cleared, so it's STILL a mixed response -> still deferred, still nothing reported. If the
    // old "divert-then-throw-for-the-remainder" behaviour were still in place, "2" would have
    // been reported twice by now; instead it is never reported while "1" keeps failing.
    intercept[FatalConnectException](writer.write(Vector(ok, poison)))

    reporter.received shouldBe empty
  }

  test(
    "dlq mode: once the retriable condition clears on a later attempt, the previously-stuck permanent error is diverted exactly once",
  ) {
    val reporter = new FakeReporter
    // A mutable script: attempt 1 is mixed (429 + 400); the retriable op then "clears" and
    // attempt 2 comes back with only the divertible 400.
    var attempt = 0
    val client = new KBulkClient {
      override def bulk(ops: Seq[BulkOp]): Try[BulkResult] = {
        attempt += 1
        val errors =
          if (attempt == 1) {
            ops.zipWithIndex.flatMap {
              case (op, idx) if op.id == "1" => Some(rejectedError().copy(index = op.index, id = op.id, position = idx))
              case (op, idx) if op.id == "2" => Some(mapperError().copy(index = op.index, id = op.id, position = idx))
              case _                         => None
            }
          } else {
            ops.zipWithIndex.collect {
              case (op, idx) if op.id == "2" => mapperError().copy(index = op.index, id = op.id, position = idx)
            }
          }
        Success(BulkResult(took = 1L, errors = errors.nonEmpty, itemErrors = errors))
      }
      override def createIndex(name: String): Try[Unit] = Success(())
      override def close(): Unit = ()
    }
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Dlq),
      dlqGate(reporter),
    )

    val poison = record("topic", "k2", struct(2, "bad"), offset = 1L)
    val ok     = record("topic", "k1", struct(1, "transient"), offset = 0L)

    // Attempt 1: mixed -> deferred, nothing reported, full set thrown.
    intercept[FatalConnectException](writer.write(Vector(ok, poison)))
    reporter.received shouldBe empty

    // Attempt 2: only the divertible 400 remains (the 429 "cleared") -> diverted, not thrown.
    val reported = writer.write(Vector(ok, poison))
    reported shouldBe Set(RecordRef.of(poison))
    reporter.received.map(_._1) shouldBe Seq(poison)
  }

  test(
    "dlq mode: when the client cannot trust positions (length mismatch -> position = -1), the whole batch throws and nothing is reported",
  ) {
    val reporter = new FakeReporter
    // A client that mimics the real clients' length-mismatch guard: every item error comes
    // back with position = -1 regardless of what ScriptedBulkClient would normally compute.
    val client = new KBulkClient {
      override def bulk(ops: Seq[BulkOp]): Try[BulkResult] =
        Success(BulkResult(took = 1L, errors = true, itemErrors = Seq(mapperError().copy(position = -1))))
      override def createIndex(name: String): Try[Unit] = Success(())
      override def close(): Unit = ()
    }
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Dlq),
      dlqGate(reporter),
    )

    intercept[FatalConnectException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    reporter.received shouldBe empty
  }

  // The "DLQ future never acknowledges" path (report() returns a Future that never completes)
  // is deliberately NOT re-tested here at the JsonBulkWriter level: JsonBulkWriter always
  // constructs its ErrantRecordHandler with the production 30-SECOND default reportTimeoutMs
  // (no override point), so exercising a real timeout here would block the test suite for 30s.
  // ErrantRecordHandlerTest already covers the timeout mechanics directly with a short bound,
  // and the position=-1 test above already proves the writer's NotReported fallback works.

  test("dlq mode: divertOne returning false for a writer-stage failure still throws the original cause") {
    class RefusingReporter extends ErrantRecordReporter {
      override def report(record: SinkRecord, error: Throwable): java.util.concurrent.Future[Void] =
        throw new RuntimeException("Tolerance exceeded in error handler")
    }
    val client = new ScriptedBulkClient(Map.empty)
    val writer = new JsonBulkWriter(
      client,
      settingsWith(
        "INSERT INTO idx SELECT * FROM topic PK id PROPERTIES('behavior.on.null.values'='FAIL')",
        ItemErrorMode.Dlq,
      ),
      DlqGate(Some(new RefusingReporter), dlqTopicConfigured = true),
    )

    // The original IllegalStateException (not a generic DLQ-failure message) must still surface.
    intercept[IllegalStateException](writer.write(Vector(tombstone("topic", "k1"))))
  }

  test("drop mode diverts a permanent item error without any DLQ configured; offsets can advance") {
    val client = new ScriptedBulkClient(Map("1" -> mapperError()))
    // No DlqGate at all — JsonBulkWriter's default constructor param is DlqGate.disabled — proving
    // drop mode needs no DLQ topic, no reporter, nothing DLQ-related.
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Drop),
    )

    val r        = record("topic", "k", struct(1, "x"))
    val reported = writer.write(Vector(r))

    reported shouldBe Set(RecordRef.of(r))
  }

  test(
    "drop mode: a mixed batch (permanent + retriable in the SAME response) drops NOTHING — diversion is deferred until a clean attempt",
  ) {
    val client = new ScriptedBulkClient(Map("1" -> rejectedError(), "2" -> mapperError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Drop),
    )

    val poison = record("topic", "k2", struct(2, "bad"), offset = 1L)
    val ok     = record("topic", "k1", struct(1, "transient"), offset = 0L)

    // See the equivalent dlq-mode test above: dropping "2" here, then throwing for "1", would
    // mean "2" gets dropped AGAIN on every redelivery while "1" keeps failing.
    val thrown = intercept[FatalConnectException](writer.write(Vector(ok, poison)))
    thrown.getMessage should include("es_rejected_execution_exception")
    thrown.getMessage should include("mapper_parsing_exception")
  }

  test("drop mode: a writer-stage failure (FAIL tombstone) is dropped instead of failing the task") {
    val client = new ScriptedBulkClient(Map.empty)
    val writer = new JsonBulkWriter(
      client,
      settingsWith(
        "INSERT INTO idx SELECT * FROM topic PK id PROPERTIES('behavior.on.null.values'='FAIL')",
        ItemErrorMode.Drop,
      ),
    )

    val ts       = tombstone("topic", "k1", offset = 3L)
    val reported = writer.write(Vector(ts))

    reported shouldBe Set(RecordRef.of(ts))
    client.calls shouldBe empty // the tombstone never became a BulkOp at all
  }

  // Regression lock for the writer-stage diversion allow-list (PerRecordWriterFailure): a
  // writer-stage failure caused by a KCQL/config-level problem — here, a PK path referencing a
  // header absent from EVERY record on the topic — is a property of the whole topic, not of any
  // one record. It must NEVER be diverted, even in drop/dlq mode, or the entire stream would be
  // silently discarded one record at a time while the task reported itself healthy.
  test(
    "drop mode: a writer-stage failure from a missing header (topic-wide, not per-record) is NEVER diverted — the task still fails",
  ) {
    val client = new ScriptedBulkClient(Map.empty)
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK _header.tenant_id", ItemErrorMode.Drop),
    )

    // Every record on "topic" is missing the tenant_id header (none of these records carry any
    // headers at all) — this is not a "which record happened to be poison" situation.
    val r1 = record("topic", "k1", struct(1, "a"))
    val r2 = record("topic", "k2", struct(2, "b"), offset = 1L)

    val thrown = intercept[IllegalArgumentException](writer.write(Vector(r1, r2)))
    thrown.getMessage should include("tenant_id")
    client.calls shouldBe empty // never even reached the bulk client — thrown before any op was built
  }

  test(
    "dlq mode: a writer-stage failure from a missing header (topic-wide, not per-record) is NEVER diverted — the task still fails",
  ) {
    val reporter = new FakeReporter
    val client   = new ScriptedBulkClient(Map.empty)
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK _header.tenant_id", ItemErrorMode.Dlq),
      dlqGate(reporter),
    )

    val r1 = record("topic", "k1", struct(1, "a"))

    intercept[IllegalArgumentException](writer.write(Vector(r1)))
    reporter.received shouldBe empty
  }

  test("drop mode: a 403 cluster_block_exception is never dropped — the batch throws instead") {
    val client = new ScriptedBulkClient(Map("1" -> clusterBlockError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Drop),
    )

    val thrown = intercept[FatalConnectException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    thrown.getMessage should include("cluster_block_exception")
  }

  test("drop mode: a 404 index_not_found_exception is never dropped — the batch throws instead") {
    val client = new ScriptedBulkClient(Map("1" -> indexNotFoundError()))
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Drop),
    )

    val thrown = intercept[FatalConnectException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    thrown.getMessage should include("index_not_found_exception")
  }

  test(
    "drop mode: when the client cannot trust positions (length mismatch -> position = -1), the whole batch throws and nothing is dropped",
  ) {
    val client = new KBulkClient {
      override def bulk(ops: Seq[BulkOp]): Try[BulkResult] =
        Success(BulkResult(took = 1L, errors = true, itemErrors = Seq(mapperError().copy(position = -1))))
      override def createIndex(name: String): Try[Unit] = Success(())
      override def close(): Unit = ()
    }
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic PK id", ItemErrorMode.Drop),
    )

    intercept[FatalConnectException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
  }

  test("a transport failure that is already a FatalConnectException passes through unchanged, not double-wrapped") {
    val original = new FatalConnectException("already fatal, do not touch")
    val client = new KBulkClient {
      override def bulk(ops:         Seq[BulkOp]): Try[BulkResult] = Failure(original)
      override def createIndex(name: String):      Try[Unit]       = Success(())
      override def close(): Unit = ()
    }
    val writer = new JsonBulkWriter(
      client,
      settingsWith("INSERT INTO idx SELECT * FROM topic", ItemErrorMode.Fail, policy = ThrowErrorPolicy()),
    )

    val thrown = intercept[FatalConnectException](writer.write(Vector(record("topic", "k", struct(1, "x")))))
    thrown shouldBe theSameInstanceAs(original)
    thrown should not be a[RetriableIntegrityException]
  }
}
