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

package com.datamountaineer.streamreactor.connect.elastic5

import java.util

import com.datamountaineer.streamreactor.connect.elastic5.config.{ElasticConfig, ElasticConfigConstants, ElasticSettings}
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.utils.{JarManifest, ProgressCounter}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._

class ElasticSinkTask extends SinkTask with StrictLogging {
  private var writer: Option[ElasticJsonWriter] = None
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * Parse the configurations and setup the writer
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/elastic-ascii.txt")).mkString + s" v $version")
    logger.info(manifest.printManifest())
    ElasticConfig.config.parse(props)
    val sinkConfig = ElasticConfig(props)
    enableProgress = sinkConfig.getBoolean(ElasticConfigConstants.PROGRESS_COUNTER_ENABLED)


    //if error policy is retry set retry interval
    val settings = ElasticSettings(sinkConfig)
    if (settings.errorPolicy.equals(Option(ErrorPolicyEnum.RETRY))) {
      context.timeout(sinkConfig.getString(ElasticConfigConstants.ERROR_RETRY_INTERVAL).toLong)
    }

    writer = Some(ElasticWriter(sinkConfig))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val seq = records.toVector
    writer.foreach(_.write(seq))

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

  override def version: String = manifest.version()
}
