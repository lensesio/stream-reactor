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

package com.datamountaineer.streamreactor.connect.hazelcast.sink

import java.util

import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.hazelcast.config.{HazelCastSinkConfig, HazelCastSinkConfigConstants, HazelCastSinkSettings}
import com.datamountaineer.streamreactor.connect.hazelcast.writers.HazelCastWriter
import com.datamountaineer.streamreactor.connect.utils.{ProgressCounter, JarManifest}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 10/08/16. 
  * stream-reactor
  */
class HazelCastSinkTask extends SinkTask with StrictLogging {
  private var writer: Option[HazelCastWriter] = None
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * Parse the configurations and setup the writer
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/hazelcast-ascii.txt")).mkString + s" v $version")
    logger.info(manifest.printManifest())

    HazelCastSinkConfig.config.parse(props)
    val sinkConfig = new HazelCastSinkConfig(props)
    enableProgress = sinkConfig.getBoolean(HazelCastSinkConfigConstants.PROGRESS_COUNTER_ENABLED)
    val settings = HazelCastSinkSettings(sinkConfig)

    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(sinkConfig.getInt(HazelCastSinkConfigConstants.ERROR_RETRY_INTERVAL).toLong)
    }

    writer = Some(HazelCastWriter(settings))
  }

  /**
    * Pass the SinkRecords to the writer
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val seq = records.toVector
    writer.foreach(w => w.write(seq))

    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  /**
    * Clean up writer
    **/
  override def stop(): Unit = {
    logger.info("Stopping Hazelcast sink.")
    writer.foreach(w => w.close())
    progressCounter.empty
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w => w.flush())
  }

  override def version: String = manifest.version()
}

