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

import ch.qos.logback.classic.{ Logger => LogbackLogger }
import ch.qos.logback.classic.{ Level => LogbackLevel }
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.lenses.streamreactor.common.errors.NoopErrorPolicy
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings
import io.lenses.streamreactor.connect.elastic.common.dlq.DlqGate
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.util.Collections.emptyMap
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.ListHasAsScala

/**
 * Unit tests for [[OpenSearchSinkTask]] covering parity rows 58, 68–70, and 79.
 *
 * Tests that require a live cluster are delegated to OpenSearchWriterFeatureIT.
 * Here we verify:
 *  - start(props) raises ConfigException when connect.opensearch.kcql is absent
 *  - flush(...) is a no-op (does not call writer.write or writer.close)
 *  - context.configs() takes precedence over props when non-empty
 *  - port=9300 migration WARN is emitted exactly once
 */
class OpenSearchSinkTaskTest extends AnyFunSuite with Matchers with MockitoSugar with ArgumentMatchersSugar {

  private val baseProps = Map(
    HOSTS   -> "localhost",
    ES_PORT -> "9200",
  )

  private def mockContext(extraConfigs: Map[String, String] = Map.empty): SinkTaskContext = {
    val ctx = mock[SinkTaskContext]
    if (extraConfigs.isEmpty)
      when(ctx.configs()).thenReturn(emptyMap[String, String])
    else
      when(ctx.configs()).thenReturn(extraConfigs.asJava)
    when(ctx.assignment()).thenReturn(java.util.Collections.emptySet[TopicPartition]())
    ctx
  }

  /**
   * A subclass that replaces createWriter with a no-op stub, letting us test lifecycle
   * methods (flush, stop, context-config precedence) without a live cluster.
   *
   * The capturedConf field captures the conf map that would have been passed to createWriter.
   */
  class StubbedTask(writerStub: JsonBulkWriter) extends OpenSearchSinkTask {
    var capturedConf: Map[String, String] = Map.empty
    override protected def createWriter(conf: Map[String, String], dlqGate: DlqGate): JsonBulkWriter = {
      capturedConf = conf
      writerStub
    }
  }

  // ---- Parity row 58 / 68: start(props) fails fast on absent KCQL ----------------------

  test("start raises ConfigException when connect.opensearch.kcql is absent") {
    val task = new OpenSearchSinkTask()
    task.initialize(mockContext())
    intercept[ConfigException] {
      task.start((baseProps + ("topics" -> "t")).asJava)
    }
  }

  private def writerStubWithSettings(): JsonBulkWriter = {
    val stub = mock[JsonBulkWriter]
    when(stub.settings).thenReturn(ElasticCommonSettings(kcqls = Seq.empty, errorPolicy = new NoopErrorPolicy))
    stub
  }

  // ---- Parity row 69: flush(...) is a no-op -----------------------------------------

  test("flush does not throw and does not interact with the writer") {
    val writerStub = writerStubWithSettings()
    val task       = new StubbedTask(writerStub)
    task.initialize(mockContext())
    task.start((baseProps + (KCQL -> "INSERT INTO idx SELECT * FROM topic")).asJava)

    noException shouldBe thrownBy {
      task.flush(emptyMap[TopicPartition, OffsetAndMetadata])
    }
    verify(writerStub, never).write(any[Vector[org.apache.kafka.connect.sink.SinkRecord]])
    verify(writerStub, never).close()
  }

  // ---- Parity row 70: context.configs() takes precedence over props ------------------

  test("context.configs() takes precedence over props when non-empty") {
    val writerStub = writerStubWithSettings()

    val contextKcql = "INSERT INTO ctx-idx SELECT * FROM ctx-topic"
    val propsKcql   = "INSERT INTO props-idx SELECT * FROM props-topic"

    // context.configs() has a complete, valid config with a different KCQL than props
    val ctxConfig = baseProps ++ Map(
      KCQL     -> contextKcql,
      "topics" -> "ctx-topic",
    )

    val task = new StubbedTask(writerStub)
    task.initialize(mockContext(ctxConfig))
    task.start((baseProps + (KCQL -> propsKcql)).asJava)

    task.capturedConf.get(KCQL) shouldBe Some(contextKcql)
    task.capturedConf.get(KCQL) should not be Some(propsKcql)
  }

  // ---- Parity row 79: port=9300 migration WARN emitted exactly once -------------------

  test("port=9300 emits exactly one migration WARN at task start") {
    // The WARN is logged in OpenSearchSinkTask.createWriter() *before* the cluster probe,
    // so it must appear even when the cluster connection subsequently fails.
    val taskLogger   = LoggerFactory.getLogger(classOf[OpenSearchSinkTask]).asInstanceOf[LogbackLogger]
    val listAppender = new ListAppender[ILoggingEvent]()
    listAppender.start()
    taskLogger.addAppender(listAppender)

    val task = new OpenSearchSinkTask()
    task.initialize(mockContext())
    try {
      task.start((baseProps ++ Map(
        KCQL    -> "INSERT INTO idx SELECT * FROM topic",
        ES_PORT -> "9300",
      )).asJava)
    } catch {
      case _: Throwable => // expected — no real cluster at localhost:9300
    } finally {
      val _ = taskLogger.detachAppender(listAppender)
    }

    val warnMessages = listAppender.list.asScala
      .filter(_.getLevel == LogbackLevel.WARN)
      .map(_.getFormattedMessage)
      .filter(_.contains("9300"))
      .toSeq

    warnMessages should have size 1
    warnMessages.head should include("9300")
  }
}
