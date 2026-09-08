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

import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Try

/**
 * End-to-end integration test exercising the full [[OpenSearchSinkTask]] lifecycle
 * (start → put → stop) against a real OpenSearch container.
 *
 * Covers the code paths exposed by defects #2 (KCQL splitter) and #3 (progress key)
 * since they are only triggered when the task starts with a real config map.
 */
class OpenSearchSinkTaskLifecycleIT extends ITBase {

  private val host      = "localhost"
  private lazy val port = container.hostNetwork.httpHostAddress.split(":").last.toInt

  private val valueSchema: Schema = SchemaBuilder.struct()
    .field("id", Schema.INT32_SCHEMA)
    .field("name", Schema.STRING_SCHEMA)
    .build()

  private def record(topic: String, id: Int, name: String, offset: Int): SinkRecord = {
    val struct = new Struct(valueSchema).put("id", id).put("name", name)
    new SinkRecord(topic, 0, Schema.STRING_SCHEMA, s"key-$id", valueSchema, struct, offset.toLong)
  }

  private def makeContext: SinkTaskContext = new SinkTaskContext {
    override def configs(): java.util.Map[String, String] = java.util.Collections.emptyMap()
    override def offset(tp:  org.apache.kafka.common.TopicPartition, l: Long): Unit = ()
    override def offset(map: java.util.Map[org.apache.kafka.common.TopicPartition, java.lang.Long]): Unit = ()
    override def timeout(l:  Long): Unit = ()
    override def assignment(): java.util.Set[org.apache.kafka.common.TopicPartition] = java.util.Collections.emptySet()
    @scala.annotation.nowarn("cat=unused")
    override def pause(partitions: org.apache.kafka.common.TopicPartition*): Unit = ()
    @scala.annotation.nowarn("cat=unused")
    override def resume(partitions: org.apache.kafka.common.TopicPartition*): Unit = ()
    override def pluginMetrics(): org.apache.kafka.common.metrics.PluginMetrics = null
    override def requestCommit(): Unit                                          = ()
  }

  test("T1: start → put → stop lifecycle succeeds with a real props map") {
    val index = "lifecycle-test"
    val props = Map(
      HOSTS   -> host,
      ES_PORT -> port.toString,
      KCQL    -> s"INSERT INTO $index SELECT * FROM topic",
    ).asJava

    val task = new OpenSearchSinkTask
    task.initialize(makeContext)
    var stopped = false
    try {
      task.start(props)

      val records = List(
        record("topic", 1, "Alice", 0),
        record("topic", 2, "Bob", 1),
      ).asJava

      noException shouldBe thrownBy(task.put(records))
      task.stop()
      stopped = true

      awaitDocumentCount(index, 2L)
    } finally if (!stopped) { val _ = Try(task.stop()) }
  }

  test("T2: multi-KCQL config (semicolon-separated) fans out to two indices end-to-end") {
    val idx1 = "lifecycle-fanout-a"
    val idx2 = "lifecycle-fanout-b"
    val kcql = s"INSERT INTO $idx1 SELECT * FROM topicA;INSERT INTO $idx2 SELECT * FROM topicB"

    val props = Map(
      HOSTS   -> host,
      ES_PORT -> port.toString,
      KCQL    -> kcql,
    ).asJava

    val task = new OpenSearchSinkTask
    task.initialize(makeContext)
    var stopped = false
    try {
      task.start(props)

      val rA = List(record("topicA", 10, "Alice", 0)).asJava
      val rB = List(record("topicB", 20, "Bob", 0)).asJava

      noException shouldBe thrownBy(task.put(rA))
      noException shouldBe thrownBy(task.put(rB))
      task.stop()
      stopped = true

      awaitDocumentCount(idx1, 1L)
      awaitDocumentCount(idx2, 1L)
    } finally if (!stopped) { val _ = Try(task.stop()) }
  }

  test("T3: progress counter key (connect.progress.enabled=true) does not crash task start") {
    val index = "lifecycle-progress"
    val props = Map(
      HOSTS                      -> host,
      ES_PORT                    -> port.toString,
      KCQL                       -> s"INSERT INTO $index SELECT * FROM topic",
      "connect.progress.enabled" -> "true",
    ).asJava

    val task = new OpenSearchSinkTask
    task.initialize(makeContext)
    var stopped = false
    try {
      noException shouldBe thrownBy(task.start(props))
      noException shouldBe thrownBy(task.put(List(record("topic", 99, "Progress", 0)).asJava))
      task.stop()
      stopped = true
    } finally if (!stopped) { val _ = Try(task.stop()) }
  }
}
