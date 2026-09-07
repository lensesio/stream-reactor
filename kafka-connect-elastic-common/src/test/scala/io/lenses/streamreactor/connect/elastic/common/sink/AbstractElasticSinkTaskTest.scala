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
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings
import io.lenses.streamreactor.connect.elastic.common.config.ItemErrorDlqMode
import io.lenses.streamreactor.connect.elastic.common.dlq.DlqGate
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
import scala.jdk.CollectionConverters._

/**
 * Unit tests for the DLQ-related behaviour of [[AbstractElasticSinkTask]]:
 *  - [[AbstractElasticSinkTask.start]] resolves and threads the [[DlqGate]] it hands to
 *    `createWriter` correctly through the full reporter/topic-configured matrix.
 *  - The three `validateDlqConfiguration` startup warnings fire under exactly the stated
 *    conditions, including the default (unset) `errors.tolerance` case.
 *  - `put()` excludes DLQ'd records from `progressCounter.update()`.
 */
class AbstractElasticSinkTaskTest extends AnyFunSuite with Matchers with MockitoSugar with ArgumentMatchersSugar {

  private val ProgressKey = "test.progress.enabled"
  private val RetryKey    = "test.retry.interval"

  /** Minimal concrete subclass that captures what would have been passed to createWriter,
    * without requiring any real Elasticsearch/OpenSearch config parsing.
    */
  private class CapturingTask(writerStub: JsonBulkWriter) extends AbstractElasticSinkTask {
    var capturedConf: Map[String, String] = Map.empty
    var capturedGate: DlqGate             = DlqGate.disabled

    override protected def asciiArtResource: String = "/does-not-exist.txt"
    override protected def errorRetryIntervalKey: String = RetryKey
    override protected def progressEnabledKey:    String = ProgressKey

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
    configs:  Map[String, String]                = Map.empty,
    reporter: Option[ErrantRecordReporter]        = None,
  ): SinkTaskContext = {
    val ctx = mock[SinkTaskContext]
    if (configs.isEmpty) when(ctx.configs()).thenReturn(emptyMap[String, String])
    else when(ctx.configs()).thenReturn(configs.asJava)
    when(ctx.assignment()).thenReturn(java.util.Collections.emptySet[TopicPartition]())
    when(ctx.errantRecordReporter()).thenReturn(reporter.orNull)
    ctx
  }

  // ---- DlqGate wiring through start() ----------------------------------------------------

  test("start() builds an enabled DlqGate and passes it to createWriter when a reporter and DLQ topic are both present") {
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

  // ---- validateDlqConfiguration warnings -------------------------------------------------

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

  private def skipSettings(strictItemErrors: Boolean = true): ElasticCommonSettings =
    ElasticCommonSettings(
      kcqls            = Seq.empty,
      errorPolicy      = new NoopErrorPolicy,
      strictItemErrors = strictItemErrors,
      itemErrorDlqMode = ItemErrorDlqMode.Skip,
    )

  test("warns when skip is set but no DLQ topic is configured") {
    val task = new CapturingTask(writerStub(skipSettings()))
    task.initialize(mockContext(reporter = Some(mock[ErrantRecordReporter])))

    val warnings = captureWarnings(task)(task.start(emptyMap[String, String]()))
    warnings.exists(_.contains(DlqGate.DlqTopicNameConfigKey)) shouldBe true
  }

  test("does not warn about a missing DLQ topic when one is actually configured") {
    val task = new CapturingTask(writerStub(skipSettings()))
    task.initialize(mockContext(
      configs  = Map(DlqGate.DlqTopicNameConfigKey -> "my-dlq-topic", DlqGate.ErrorsToleranceConfigKey -> "all"),
      reporter = Some(mock[ErrantRecordReporter]),
    ))

    val warnings = captureWarnings(task)(task.start(emptyMap[String, String]()))
    warnings.exists(_.contains(DlqGate.DlqTopicNameConfigKey)) shouldBe false
  }

  test("warns when skip is set together with bulk.strict.item.errors=false") {
    val task = new CapturingTask(writerStub(skipSettings(strictItemErrors = false)))
    task.initialize(mockContext(
      configs  = Map(DlqGate.DlqTopicNameConfigKey -> "my-dlq-topic", DlqGate.ErrorsToleranceConfigKey -> "all"),
      reporter = Some(mock[ErrantRecordReporter]),
    ))

    val warnings = captureWarnings(task)(task.start(emptyMap[String, String]()))
    warnings.exists(_.contains("bulk.strict.item.errors=false")) shouldBe true
  }

  test("does not warn about bulk.strict.item.errors when it is true (the default)") {
    val task = new CapturingTask(writerStub(skipSettings(strictItemErrors = true)))
    task.initialize(mockContext(
      configs  = Map(DlqGate.DlqTopicNameConfigKey -> "my-dlq-topic", DlqGate.ErrorsToleranceConfigKey -> "all"),
      reporter = Some(mock[ErrantRecordReporter]),
    ))

    val warnings = captureWarnings(task)(task.start(emptyMap[String, String]()))
    warnings.exists(_.contains("bulk.strict.item.errors=false")) shouldBe false
  }

  test("warns when skip is set and errors.tolerance is explicitly none") {
    val task = new CapturingTask(writerStub(skipSettings()))
    task.initialize(mockContext(
      configs  = Map(DlqGate.DlqTopicNameConfigKey -> "my-dlq-topic", DlqGate.ErrorsToleranceConfigKey -> "none"),
      reporter = Some(mock[ErrantRecordReporter]),
    ))

    val warnings = captureWarnings(task)(task.start(emptyMap[String, String]()))
    warnings.exists(_.contains(DlqGate.ErrorsToleranceConfigKey)) shouldBe true
  }

  // Regression test for the fix: Kafka Connect's own default for errors.tolerance is "none" —
  // an operator who sets the DLQ topic and skip but simply OMITS errors.tolerance is on exactly
  // the same footing as one who set it to "none" explicitly, and must get the same warning.
  test("warns when skip is set and errors.tolerance is left UNSET (defaults to none)") {
    val task = new CapturingTask(writerStub(skipSettings()))
    task.initialize(mockContext(
      configs  = Map(DlqGate.DlqTopicNameConfigKey -> "my-dlq-topic"),
      reporter = Some(mock[ErrantRecordReporter]),
    ))

    val warnings = captureWarnings(task)(task.start(emptyMap[String, String]()))
    warnings.exists(_.contains(DlqGate.ErrorsToleranceConfigKey)) shouldBe true
  }

  test("does not warn about errors.tolerance when it is explicitly set to all") {
    val task = new CapturingTask(writerStub(skipSettings()))
    task.initialize(mockContext(
      configs  = Map(DlqGate.DlqTopicNameConfigKey -> "my-dlq-topic", DlqGate.ErrorsToleranceConfigKey -> "all"),
      reporter = Some(mock[ErrantRecordReporter]),
    ))

    val warnings = captureWarnings(task)(task.start(emptyMap[String, String]()))
    warnings.exists(_.contains(DlqGate.ErrorsToleranceConfigKey)) shouldBe false
  }

  test("emits no DLQ-related warnings at all when itemErrorDlqMode is Fail (the default)") {
    val failSettings = ElasticCommonSettings(kcqls = Seq.empty, errorPolicy = new NoopErrorPolicy)
    val task         = new CapturingTask(writerStub(failSettings))
    task.initialize(mockContext())

    val warnings = captureWarnings(task)(task.start(emptyMap[String, String]()))
    warnings shouldBe empty
  }

  // ---- put() progress-counter exclusion ---------------------------------------------------

  private def sinkRecord(topic: String, offset: Long): SinkRecord =
    new SinkRecord(topic, 0, Schema.STRING_SCHEMA, "k", Schema.STRING_SCHEMA, "v", offset)

  test("put() excludes DLQ'd records from the progress counter") {
    val goodTopic = "good-topic"
    val dlqTopic  = "dlq-topic"
    val good1     = sinkRecord(goodTopic, 0L)
    val good2     = sinkRecord(goodTopic, 1L)
    val dlqd      = sinkRecord(dlqTopic, 0L)

    val stub = mock[JsonBulkWriter]
    when(stub.settings).thenReturn(ElasticCommonSettings(kcqls = Seq.empty, errorPolicy = new NoopErrorPolicy))
    when(stub.write(any[Vector[SinkRecord]])).thenReturn(Set(dlqd))

    val task = new CapturingTask(stub)
    task.initialize(mockContext())
    // ProgressCounter logs a "Delivered [n] records for [topic]" line the first time update()
    // is called with a non-empty collection (its internal timestamp starts at 0, so the very
    // first elapsed-time check always trips) — a convenient black-box observation point without
    // reaching into ProgressCounter's private state.
    task.start(Map(ProgressKey -> "true").asJava)

    val progressLogger = LoggerFactory.getLogger(
      classOf[io.lenses.streamreactor.common.utils.ProgressCounter],
    ).asInstanceOf[LogbackLogger]
    val listAppender = new ListAppender[ILoggingEvent]()
    listAppender.start()
    progressLogger.addAppender(listAppender)
    try {
      task.put(java.util.List.of(good1, good2, dlqd))

      val infoLines = listAppender.list.asScala.map(_.getFormattedMessage).toSeq
      infoLines.exists(l => l.contains("[2]") && l.contains(goodTopic)) shouldBe true
      infoLines.exists(_.contains(dlqTopic)) shouldBe false
    } finally {
      val _ = progressLogger.detachAppender(listAppender)
    }
  }

  test("put() counts everything when the writer reports no DLQ'd records") {
    val topic  = "some-topic"
    val r1     = sinkRecord(topic, 0L)
    val r2     = sinkRecord(topic, 1L)

    val stub = mock[JsonBulkWriter]
    when(stub.settings).thenReturn(ElasticCommonSettings(kcqls = Seq.empty, errorPolicy = new NoopErrorPolicy))
    when(stub.write(any[Vector[SinkRecord]])).thenReturn(Set.empty[SinkRecord])

    val task = new CapturingTask(stub)
    task.initialize(mockContext())
    task.start(Map(ProgressKey -> "true").asJava)

    val progressLogger = LoggerFactory.getLogger(
      classOf[io.lenses.streamreactor.common.utils.ProgressCounter],
    ).asInstanceOf[LogbackLogger]
    val listAppender = new ListAppender[ILoggingEvent]()
    listAppender.start()
    progressLogger.addAppender(listAppender)
    try {
      task.put(java.util.List.of(r1, r2))

      val infoLines = listAppender.list.asScala.map(_.getFormattedMessage).toSeq
      infoLines.exists(l => l.contains("[2]") && l.contains(topic)) shouldBe true
    } finally {
      val _ = progressLogger.detachAppender(listAppender)
    }
  }

  test("put() does not touch the progress counter at all when progress is disabled") {
    val topic = "some-topic"
    val r1    = sinkRecord(topic, 0L)

    val stub = mock[JsonBulkWriter]
    when(stub.settings).thenReturn(ElasticCommonSettings(kcqls = Seq.empty, errorPolicy = new NoopErrorPolicy))
    when(stub.write(any[Vector[SinkRecord]])).thenReturn(Set.empty[SinkRecord])

    val task = new CapturingTask(stub)
    task.initialize(mockContext())
    // progressEnabledKey deliberately left unset -> enableProgress stays false.
    task.start(emptyMap[String, String]())

    val progressLogger = LoggerFactory.getLogger(
      classOf[io.lenses.streamreactor.common.utils.ProgressCounter],
    ).asInstanceOf[LogbackLogger]
    val listAppender = new ListAppender[ILoggingEvent]()
    listAppender.start()
    progressLogger.addAppender(listAppender)
    try {
      task.put(java.util.List.of(r1))
      listAppender.list.asScala shouldBe empty
    } finally {
      val _ = progressLogger.detachAppender(listAppender)
    }
  }
}
