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

package com.datamountaineer.streamreactor.connect.voltdb

import java.util

import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.utils.{ProgressCounter, JarManifest}
import com.datamountaineer.streamreactor.connect.voltdb.config.{VoltSettings, VoltSinkConfig, VoltSinkConfigConstants}
import com.datamountaineer.streamreactor.connect.voltdb.writers.VoltDbWriter
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._

/**
  * <h1>VoltSinkTask</h1>
  *
  * Kafka Connect VoltDb sink task. Called by framework to put records to the target database
  **/
class VoltSinkTask extends SinkTask with StrictLogging {

  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  var writer: Option[VoltDbWriter] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * Parse the configurations and setup the writer
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/voltdb-ascii.txt")).mkString + s" v $version")
    logger.info(manifest.printManifest())

    VoltSinkConfig.config.parse(props)
    val sinkConfig = VoltSinkConfig(props)
    val voltSettings = VoltSettings(sinkConfig)
    enableProgress = sinkConfig.getBoolean(VoltSinkConfigConstants.PROGRESS_COUNTER_ENABLED)


    //if error policy is retry set retry interval
    if (voltSettings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(sinkConfig.getInt(VoltSinkConfigConstants.ERROR_RETRY_INTERVAL_CONFIG).toLong)
    }
    writer = Some(new VoltDbWriter(voltSettings))

  }

  /**
    * Pass the SinkRecords to the writer for Writing
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    if (records.size() == 0) {
      logger.info("Empty list of records received.")
    }
    else {
      require(writer.nonEmpty, "Writer is not set!")
      val seq = records.toVector
      writer.foreach(w => w.write(seq))
      if (enableProgress) {
        progressCounter.update(seq)
      }
    }
  }

  /**
    * Clean up Volt connections
    **/
  override def stop(): Unit = {
    logger.info("Stopping VoltDb sink.")
    writer.foreach(w => w.close())
  }

  override def version: String = manifest.version()

  override def flush(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}
}
