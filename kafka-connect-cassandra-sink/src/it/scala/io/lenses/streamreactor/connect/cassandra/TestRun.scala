/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cassandra

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito.mock

import java.util
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class TestRun(properties: Map[String, String]) extends StrictLogging with AutoCloseable {
  val conn = new CassandraSinkConnector
  val task = new CassandraSinkTask
  val taskContext: SinkTaskContext = mock(classOf[SinkTaskContext])
  task.initialize(taskContext)

  def start(): Unit = {
    conn.start(properties.asJava)
    val taskProps: util.List[util.Map[String, String]] = conn.taskConfigs(1)
    task.start(taskProps.get(0))
  }

  override def close(): Unit = {
    task.stop()
    conn.stop()
  }

  def put(record: SinkRecord*): Unit =
    task.put(record.toList.asJava)

}
