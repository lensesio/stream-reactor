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
package io.lenses.streamreactor.connect.cassandra

import com.datastax.oss.common.sink.AbstractSinkRecord
import com.datastax.oss.common.sink.AbstractSinkTask
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.connect.cassandra.adapters.RecordAdapter
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkTaskContext

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

class CassandraProcessorException(message: String, cause: Throwable) extends ConnectException(message, cause)

class CassandraProcessor(
  appName:        String,
  errorMode:      IgnoredErrorMode,
  failureOffsets: mutable.Map[TopicPartition, OffsetAndMetadata],
  context:        SinkTaskContext,
  errors:         AtomicReference[List[CassandraProcessorException]],
) extends AbstractSinkTask
    with JarManifestProvided
    with StrictLogging {

  def getFailureOffsets: mutable.Map[TopicPartition, OffsetAndMetadata] = failureOffsets

  override def start(props: java.util.Map[String, String]): Unit = {
    super.start(props)
    logger.info(s"Starting Cassandra Processor for application: $appName")
    failureOffsets.clear()
  }

  override def beforeProcessingBatch(): Unit = {
    super.beforeProcessingBatch()
    failureOffsets.clear()
  }

  override def handleFailure(record: AbstractSinkRecord, e: Throwable, cql: String, failCounter: Runnable): Unit = {
    val sinkRecord      = record.asInstanceOf[RecordAdapter].record
    val isDriverFailure = cql != null
    if (
      errorMode == IgnoredErrorMode.None ||
      (!isDriverFailure && errorMode == IgnoredErrorMode.Driver)
    ) {
      val topicPartition = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition())
      var currentOffset  = Long.MaxValue
      if (failureOffsets.contains(topicPartition)) {
        currentOffset = failureOffsets(topicPartition).offset()
      }
      if (sinkRecord.kafkaOffset < currentOffset) {
        failureOffsets.put(topicPartition, new OffsetAndMetadata(sinkRecord.kafkaOffset))
        context.offset(topicPartition, sinkRecord.kafkaOffset())
      }
    }
    failCounter.run()
    val errorDesc =
      if (isDriverFailure) {
        s"Failed to process record topic:${sinkRecord.topic()}  partition:${sinkRecord.kafkaPartition()} offset:${sinkRecord.kafkaOffset()} with cql statement: $cql"
      } else {
        s"Failed to process record topic:${sinkRecord.topic()}  partition:${sinkRecord.kafkaPartition()} offset:${sinkRecord.kafkaOffset()} "
      }
    logger.error(errorDesc, e)
    errors.updateAndGet(current => new CassandraProcessorException(errorDesc, e) :: current)
    ()
  }

  override def applicationName(): String = appName
}
