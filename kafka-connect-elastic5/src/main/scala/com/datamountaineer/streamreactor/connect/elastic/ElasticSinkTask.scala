/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.elastic

import java.util

import com.datamountaineer.streamreactor.connect.elastic.config.{ElasticSinkConfig, ElasticSinkConfigConstants}
import com.datamountaineer.streamreactor.connect.utils.ProgressCounter
import com.sksamuel.elastic4s.xpack.security.XPackElasticClient
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._

class ElasticSinkTask extends SinkTask with StrictLogging {
  private var writer: Option[ElasticJsonWriter] = None
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false

  /**
    * Parse the configurations and setup the writer
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/elastic-ascii.txt")).mkString)

    ElasticSinkConfig.config.parse(props)
    val sinkConfig = ElasticSinkConfig(props)
    enableProgress = sinkConfig.getBoolean(ElasticSinkConfigConstants.PROGRESS_COUNTER_ENABLED)
    writer = Some(ElasticWriter(config = sinkConfig, context = context))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w => w.write(records.toSet))
    val seq = records.toVector
    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  /**
    * Clean up writer
    **/
  override def stop(): Unit = {
    logger.info("Stopping Elastic sink.")
    writer.foreach(w => w.close())
    progressCounter.empty
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    logger.info("Flushing Elastic Sink")
  }

  override def version(): String = getClass.getPackage.getImplementationVersion
}
