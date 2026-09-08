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
package io.lenses.streamreactor.connect.elastic.common.sink

import ch.qos.logback.classic.{ Logger => LogbackLogger }
import ch.qos.logback.classic.{ Level => LogbackLevel }
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.lenses.streamreactor.common.errors.NoopErrorPolicy
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemError
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkOp
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkResult
import io.lenses.streamreactor.connect.elastic.common.bulk.KBulkClient
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings
import io.lenses.streamreactor.connect.elastic.common.config.ItemErrorMode
import io.lenses.streamreactor.connect.elastic.common.dlq.DlqGate
import io.lenses.streamreactor.connect.elastic.common.dlq.RecordRef
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.util.Collections.emptyMap
import java.util.concurrent.CompletableFuture
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Success
import scala.util.Try

/**
 * Unit tests for the item-error-diversion behaviour of [[AbstractElasticSinkTask]]:
 *  - [[AbstractElasticSinkTask.start]] resolves and threads the [[DlqGate]] it hands to
 *    `createWriter` correctly through the full reporter/topic-configured matrix.
 *  - The `validateItemErrorConfiguration` startup warnings fire under exactly the stated
 *    conditions for both `dlq` mode (three warnings, including the default/unset
 *    `errors.tolerance` case) and `drop` mode (the deliberate discard warning, plus the
 *    `bulk.strict.item.errors=false` warning it shares with `dlq`).
 *  - `put()` excludes diverted records from `progressCounter.update()`.
 */
class AbstractElasticSinkTaskTest extends AnyFunSuite with Matchers with MockitoSugar with ArgumentMatchersSugar {

  private val ProgressKey = "test.progress.enabled"
  private val RetryKey    = "test.retry.interval"

  /**
   * Minimal concrete subclass that captures what would have been passed to createWriter,
   * without requiring any real Elasticsearch/OpenSearch config parsing.
   */
  private val ItemErrorModeKey    = "test.bulk.item.errors"
  private val StrictItemErrorsKey = "test.bulk.strict.item.errors"

  private class CapturingTask(writerStub: JsonBulkWriter, diversionSupported: Boolean = true) extends AbstractElasticSinkTask {
    var capturedConf: Map[String, String] = Map.empty
    var capturedGate: DlqGate             = DlqGate.disabled

    override protected def asciiArtResource:              String  = "/does-not-exist.txt"
    override protected def errorRetryIntervalKey:         String  = RetryKey
    override protected def progressEnabledKey:            String  = ProgressKey
    override protected def itemErrorModeConfigKey:        String  = ItemErrorModeKey
    override protected def strictItemErrorsConfigKey:     String  = StrictItemErrorsKey
    override protected def supportsItemErrorDiversion:    Boolean = diversionSupported

    override protected def createWriter(conf: Map[String, String], dlqGate: DlqGate): JsonBulkWriter = {
      capturedConf = conf
      capturedGate = dlqGate
      writerStub
    }
  }

  private val defaultSettings: ElasticCommonSettings =
    ElasticCommonSettings(kcqls = Seq.empty, errorPolicy = new NoopErrorPolicy)

  private def writerStub(settings: ElasticCommonSettings = defaultSettings): JsonBulkWriter = {
    val stub = mock[JsonBulkWriter]
    when(stub.settings).thenReturn(settings)
    stub
  }

  private def mockContext(
    configs:  Map[String, String]          = Map.empty,
    reporter: Option[ErrantRecordReporter] = None,
  ): SinkTaskContext = {
    val ctx = mock[SinkTaskContext]
    if (configs.isEmpty) when(ctx.configs()).thenReturn(emptyMap[String, String])
    else when(ctx.configs()).thenReturn(configs.asJava)
    when(ctx.assignment()).thenReturn(java.util.Collections.emptySet[TopicPartition]())
    when(ctx.errantRecordReporter()).thenReturn(reporter.orNull)
    ctx
  }

  test(
    "start() builds an enabled DlqGate and passes it to createWriter when a reporter and DLQ topic are both present",
  ) {
    val task = new CapturingTask(writerStub())
    task.initialize(mockContext(
      configs  = Map(DlqGate.DlqTopicNameConfigKey -> "my-dlq-topic"),
      reporter = Some(mock[ErrantRecordReporter]),
    ))
    task.start(emptyMap[String, String]())

    task.capturedGate.enabled shouldBe true
    task.capturedGate.reporter shouldBe defined
    task.capturedGate.dlqTopicConfigured shouldBe true
  }

  test("start() builds a disabled DlqGate when the reporter is present but no DLQ topic is configured") {
    val task = new CapturingTask(writerStub())
    task.initialize(mockContext(reporter = Some(mock[ErrantRecordReporter])))
    task.start(emptyMap[String, String]())

    task.capturedGate.enabled shouldBe false
    task.capturedGate.reporter shouldBe defined
  }

  test("start() builds a disabled DlqGate when no reporter is present, even if a DLQ topic is configured") {
    val task = new CapturingTask(writerStub())
    task.initialize(mockContext(configs = Map(DlqGate.DlqTopicNameConfigKey -> "my-dlq-topic")))
    task.start(emptyMap[String, String]())

    task.capturedGate.enabled shouldBe false
    task.capturedGate.reporter shouldBe empty
  }

  test("start() passes the resolved config map (from context.configs()) through to createWriter") {
    val task = new CapturingTask(writerStub())
    task.initialize(mockContext(configs = Map("some.key" -> "some.value")))
    task.start(emptyMap[String, String]())

    task.capturedConf.get("some.key") shouldBe Some("some.value")
  }

  // StrictLogging resolves `logger` from the RUNTIME class (getClass), not the class that
  // declares the logging call — since CapturingTask is a subclass of AbstractElasticSinkTask,
  // its WARN logs are emitted under CapturingTask's logger name, not AbstractElasticSinkTask's.
  private def captureWarnings(task: CapturingTask)(body: => Unit): Seq[String] = {
    val taskLogger   = LoggerFactory.getLogger(task.getClass).asInstanceOf[LogbackLogger]
    val listAppender = new ListAppender[ILoggingEvent]()
    listAppender.start()
    taskLogger.addAppender(listAppender)
    try {
      body
      listAppender.list.asScala.filter(_.getLevel == LogbackLevel.WARN).map(_.getFormattedMessage).toSeq
    } finally {
      val _ = taskLogger.detachAppender(listAppender)
    }
  }

  private def dlqSettings(strictItemErrors: Boolean = true): ElasticCommonSettings =
    ElasticCommonSettings(
      kcqls            = Seq.empty,
      errorPolicy      = new NoopErrorPolicy,
      strictItemErrors = strictItemErrors,
      itemErrorMode    = ItemErrorMode.Dlq,
    )

  private def dropSettings(strictItemErrors: Boolean = true): ElasticCommonSettings =
    ElasticCommonSettings(
      kcqls            = Seq.empty,
      errorPolicy      = new NoopErrorPolicy,
      strictItemErrors = strictItemErrors,
      itemErrorMode    = ItemErrorMode.Drop,
    )

  // NOTE: validateItemErrorConfiguration (the source of every warning tested below) now reads
  // bulk.item.errors/bulk.strict.item.errors directly from confMap via itemErrorModeConfigKey /
  // strictItemErrorsConfigKey — NOT from the writer's (already-parsed) settings — specifically
  // so it can run, and its warnings be guaranteed visible, BEFORE createWriter (see
  // AbstractElasticSinkTask.start). `configs` in each [[WarnCase]] below is therefore the thing
  // that actually drives these warnings; `settings` is kept in sync with it purely so w.settings
  // (used elsewhere in start(), e.g. errorPolicy) agrees with what confMap says.

  /**
   * One row of the startup-warning matrix. `expect`/`forbid` are substrings that MUST/must-NOT
   * appear in some captured WARN line — every substring here is a stable identifier (a
   * `DlqGate` config-key constant, or the fixed `key=value` token the production code emits),
   * never free-form prose, so a copy-edit of the surrounding sentence can't break these.
   */
  private case class WarnCase(
    name:               String,
    settings:           ElasticCommonSettings,
    configs:            Map[String, String],
    reporter:           Boolean     = true,
    expect:             Seq[String] = Seq.empty,
    forbid:             Seq[String] = Seq.empty,
    diversionSupported: Boolean     = true,
  )

  private val warnCases: Seq[WarnCase] = Seq(
    WarnCase(
      "warns when dlq is set but no DLQ topic is configured",
      dlqSettings(),
      Map(ItemErrorModeKey -> "dlq"),
      expect = Seq(DlqGate.DlqTopicNameConfigKey),
    ),
    WarnCase(
      // A DLQ topic IS configured, but no reporter was handed out (old Connect runtime): dlq
      // still degrades to fail, so this must warn — and NOT via the "topic not configured" message.
      "warns when dlq is set with a DLQ topic configured but no ErrantRecordReporter provided",
      dlqSettings(),
      Map(ItemErrorModeKey                 -> "dlq",
          DlqGate.DlqTopicNameConfigKey    -> "my-dlq-topic",
          DlqGate.ErrorsToleranceConfigKey -> "all",
      ),
      reporter = false,
      expect   = Seq("does not support per-record error reporting"),
      forbid   = Seq(DlqGate.DlqTopicNameConfigKey),
    ),
    WarnCase(
      "does not warn about a missing DLQ topic when one is actually configured",
      dlqSettings(),
      Map(ItemErrorModeKey                 -> "dlq",
          DlqGate.DlqTopicNameConfigKey    -> "my-dlq-topic",
          DlqGate.ErrorsToleranceConfigKey -> "all",
      ),
      forbid = Seq(DlqGate.DlqTopicNameConfigKey),
    ),
    WarnCase(
      "warns when dlq is set together with bulk.strict.item.errors=false",
      dlqSettings(strictItemErrors = false),
      Map(
        ItemErrorModeKey                 -> "dlq",
        StrictItemErrorsKey              -> "false",
        DlqGate.DlqTopicNameConfigKey    -> "my-dlq-topic",
        DlqGate.ErrorsToleranceConfigKey -> "all",
      ),
      expect = Seq("bulk.strict.item.errors=false"),
    ),
    WarnCase(
      "does not warn about bulk.strict.item.errors when it is true (the default)",
      dlqSettings(strictItemErrors = true),
      Map(ItemErrorModeKey                 -> "dlq",
          DlqGate.DlqTopicNameConfigKey    -> "my-dlq-topic",
          DlqGate.ErrorsToleranceConfigKey -> "all",
      ),
      forbid = Seq("bulk.strict.item.errors=false"),
    ),
    WarnCase(
      "warns when dlq is set and errors.tolerance is explicitly none",
      dlqSettings(),
      Map(ItemErrorModeKey                 -> "dlq",
          DlqGate.DlqTopicNameConfigKey    -> "my-dlq-topic",
          DlqGate.ErrorsToleranceConfigKey -> "none",
      ),
      expect = Seq(DlqGate.ErrorsToleranceConfigKey),
    ),
    // Kafka Connect's default for errors.tolerance is "none" — omitting it is equivalent to
    // setting it to "none" explicitly, and must produce the same warning.
    WarnCase(
      "warns when dlq is set and errors.tolerance is left UNSET (defaults to none)",
      dlqSettings(),
      Map(ItemErrorModeKey -> "dlq", DlqGate.DlqTopicNameConfigKey -> "my-dlq-topic"),
      expect = Seq(DlqGate.ErrorsToleranceConfigKey),
    ),
    WarnCase(
      "does not warn about errors.tolerance when it is explicitly set to all",
      dlqSettings(),
      Map(ItemErrorModeKey                 -> "dlq",
          DlqGate.DlqTopicNameConfigKey    -> "my-dlq-topic",
          DlqGate.ErrorsToleranceConfigKey -> "all",
      ),
      forbid = Seq(DlqGate.ErrorsToleranceConfigKey),
    ),
    WarnCase(
      "emits no item-error warnings at all when itemErrorMode is Fail (the default)",
      ElasticCommonSettings(kcqls = Seq.empty, errorPolicy = new NoopErrorPolicy),
      Map.empty,
      reporter = false,
      forbid = Seq(DlqGate.DlqTopicNameConfigKey,
                   DlqGate.ErrorsToleranceConfigKey,
                   "bulk.item.errors=drop",
                   "bulk.strict.item.errors=false",
      ),
    ),
    WarnCase(
      "warns that drop discards records, unconditionally, as soon as drop is set",
      dropSettings(),
      Map(ItemErrorModeKey -> "drop"),
      reporter = false,
      expect   = Seq("bulk.item.errors=drop"),
    ),
    WarnCase(
      "drop mode does not warn about the DLQ topic or errors.tolerance — neither applies to drop",
      dropSettings(),
      Map(ItemErrorModeKey -> "drop"),
      reporter = false,
      forbid   = Seq(DlqGate.DlqTopicNameConfigKey, DlqGate.ErrorsToleranceConfigKey),
    ),
    WarnCase(
      "drop mode warns about bulk.strict.item.errors=false too — nothing would ever surface to drop",
      dropSettings(strictItemErrors = false),
      Map(ItemErrorModeKey -> "drop", StrictItemErrorsKey -> "false"),
      reporter = false,
      expect   = Seq("bulk.strict.item.errors=false"),
    ),
    WarnCase(
      "drop mode does not warn about bulk.strict.item.errors when it is true (the default)",
      dropSettings(strictItemErrors = true),
      Map(ItemErrorModeKey -> "drop"),
      reporter = false,
      forbid   = Seq("bulk.strict.item.errors=false"),
    ),
    // --- supportsItemErrorDiversion = false (ES6 scenario) ---
    WarnCase(
      "when diversion is unsupported, drop emits an 'ignored' WARN instead of the data-loss WARN",
      dropSettings(),
      Map(ItemErrorModeKey -> "drop"),
      reporter           = false,
      diversionSupported = false,
      expect             = Seq("setting is ignored"),
      forbid             = Seq("is active", "silently discarded"),
    ),
    WarnCase(
      "when diversion is unsupported, dlq emits an 'ignored' WARN instead of the DLQ warnings",
      dlqSettings(),
      Map(ItemErrorModeKey -> "dlq"),
      diversionSupported = false,
      expect             = Seq("setting is ignored"),
      forbid             = Seq(DlqGate.DlqTopicNameConfigKey, DlqGate.ErrorsToleranceConfigKey),
    ),
    WarnCase(
      "when diversion is unsupported and mode is fail (the default), no warnings are emitted",
      ElasticCommonSettings(kcqls = Seq.empty, errorPolicy = new NoopErrorPolicy),
      Map.empty,
      reporter           = false,
      diversionSupported = false,
      forbid             = Seq("setting is ignored", "bulk.item.errors=drop", DlqGate.DlqTopicNameConfigKey),
    ),
  )

  warnCases.foreach { c =>
    test(c.name) {
      val task = new CapturingTask(writerStub(c.settings), diversionSupported = c.diversionSupported)
      task.initialize(mockContext(
        configs  = c.configs,
        reporter = if (c.reporter) Some(mock[ErrantRecordReporter]) else None,
      ))

      val warnings = captureWarnings(task)(task.start(emptyMap[String, String]()))
      c.expect.foreach(s =>
        withClue(s"expected a WARN containing '$s': ")(warnings.exists(_.contains(s)) shouldBe true),
      )
      c.forbid.foreach(s =>
        withClue(s"did not expect any WARN containing '$s': ")(warnings.exists(_.contains(s)) shouldBe false),
      )
    }
  }

  // Value must be valid, NON-EMPTY JSON (schema-less string path goes straight to
  // Transform/Jackson, and an empty object is treated as a tombstone by processRecord) — used
  // both by mocked-writer tests (content irrelevant) and the real-JsonBulkWriter test below
  // (content must actually parse to a non-empty document).
  private def sinkRecord(topic: String, offset: Long): SinkRecord =
    new SinkRecord(topic, 0, Schema.STRING_SCHEMA, "k", Schema.STRING_SCHEMA, """{"v":1}""", offset)

  /** A [[KBulkClient]] that fails the given op ids with a permanent (400) item error; everything else succeeds. */
  private class PoisonIdBulkClient(poisonIds: Set[String]) extends KBulkClient {
    val calls: mutable.Buffer[Seq[BulkOp]] = mutable.Buffer.empty
    override def bulk(ops: Seq[BulkOp]): Try[BulkResult] = {
      calls += ops
      val errors = ops.zipWithIndex.collect {
        case (op, idx) if poisonIds(op.id) =>
          BulkItemError(index     = op.index,
                        id        = op.id,
                        reason    = "boom",
                        errorType = "mapper_parsing_exception",
                        status    = 400,
                        position  = idx,
          )
      }
      Success(BulkResult(took = 1L, errors = errors.nonEmpty, itemErrors = errors))
    }
    override def createIndex(name: String): Try[Unit] = Success(())
    override def close(): Unit = ()
  }

  private class AlwaysAckReporter extends ErrantRecordReporter {
    override def report(record: SinkRecord, error: Throwable): java.util.concurrent.Future[Void] =
      CompletableFuture.completedFuture[Void](null)
  }

  // Drives this through a REAL JsonBulkWriter (dlq mode) backed by a scripted KBulkClient,
  // rather than mocking JsonBulkWriter.write directly — the point of this test is that put()
  // correctly excludes whatever the writer ACTUALLY diverted, not that put() can filter
  // whatever a mock is told to return.
  test("put() excludes DLQ'd records from the progress counter") {
    val goodTopic = "good-topic"
    val dlqTopic  = "dlq-topic"
    val good1     = sinkRecord(goodTopic, 0L)
    val good2     = sinkRecord(goodTopic, 1L)
    val dlqd      = sinkRecord(dlqTopic, 0L)

    // INSERT with no PK -> autoGenId = topic.partition.offset, joined with the default "-".
    val poisonId = s"$dlqTopic-0-0"
    val client   = new PoisonIdBulkClient(Set(poisonId))
    val settings = ElasticCommonSettings(
      kcqls = Seq(
        io.lenses.kcql.Kcql.parse(s"INSERT INTO idx SELECT * FROM $goodTopic"),
        io.lenses.kcql.Kcql.parse(s"INSERT INTO idx SELECT * FROM $dlqTopic"),
      ),
      errorPolicy   = new NoopErrorPolicy,
      itemErrorMode = ItemErrorMode.Dlq,
    )

    class RealWriterTask extends AbstractElasticSinkTask {
      override protected def asciiArtResource:          String = "/does-not-exist.txt"
      override protected def errorRetryIntervalKey:     String = RetryKey
      override protected def progressEnabledKey:        String = ProgressKey
      override protected def itemErrorModeConfigKey:    String = ItemErrorModeKey
      override protected def strictItemErrorsConfigKey: String = StrictItemErrorsKey
      override protected def createWriter(conf: Map[String, String], dlqGate: DlqGate): JsonBulkWriter =
        new JsonBulkWriter(client, settings, dlqGate)
    }

    val task = new RealWriterTask
    task.initialize(
      mockContext(
        // ProgressKey must be in context.configs() (takes precedence over props when non-empty)
        // alongside the DLQ-related keys already carried by this mockContext.
        configs = Map(
          ItemErrorModeKey                 -> "dlq",
          DlqGate.DlqTopicNameConfigKey    -> "my-dlq-topic",
          DlqGate.ErrorsToleranceConfigKey -> "all",
          ProgressKey                      -> "true",
        ),
        reporter = Some(new AlwaysAckReporter),
      ),
    )
    task.start(Map(ProgressKey -> "true").asJava)

    task.put(java.util.List.of(good1, good2, dlqd))

    task.progressCounter.countsByTopic.get(goodTopic) shouldBe Some(2L)
    task.progressCounter.countsByTopic.get(dlqTopic) shouldBe None
  }

  test("put() counts everything when the writer reports no DLQ'd records") {
    val topic = "some-topic"
    val r1    = sinkRecord(topic, 0L)
    val r2    = sinkRecord(topic, 1L)

    val stub = mock[JsonBulkWriter]
    when(stub.settings).thenReturn(ElasticCommonSettings(kcqls = Seq.empty, errorPolicy = new NoopErrorPolicy))
    when(stub.write(any[Vector[SinkRecord]])).thenReturn(Set.empty[RecordRef])

    val task = new CapturingTask(stub)
    task.initialize(mockContext())
    task.start(Map(ProgressKey -> "true").asJava)

    task.put(java.util.List.of(r1, r2))

    task.progressCounter.countsByTopic.get(topic) shouldBe Some(2L)
  }

  test("put() does not touch the progress counter at all when progress is disabled") {
    val topic = "some-topic"
    val r1    = sinkRecord(topic, 0L)

    val stub = mock[JsonBulkWriter]
    when(stub.settings).thenReturn(ElasticCommonSettings(kcqls = Seq.empty, errorPolicy = new NoopErrorPolicy))
    when(stub.write(any[Vector[SinkRecord]])).thenReturn(Set.empty[RecordRef])

    val task = new CapturingTask(stub)
    task.initialize(mockContext())
    // progressEnabledKey deliberately left unset -> enableProgress stays false.
    task.start(emptyMap[String, String]())

    task.put(java.util.List.of(r1))

    task.progressCounter.countsByTopic shouldBe empty
  }
}
