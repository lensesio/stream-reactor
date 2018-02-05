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

package com.datamountaineer.streamreactor.connect.druid

import java.util

import com.datamountaineer.streamreactor.connect.druid.config._
import com.datamountaineer.streamreactor.connect.druid.writer.DruidDbWriter
import com.datamountaineer.streamreactor.connect.utils.{ProgressCounter, JarManifest}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 04/03/16. 
  * stream-reactor
  */
class DruidSinkTask extends SinkTask with StrictLogging {
  var writer: Option[DruidDbWriter] = None
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * Parse the configurations and setup the writer
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/druid-ascii.txt")).mkString + s" v $version")
    logger.info(manifest.printManifest())
    DruidConfig.config.parse(props)
    val sinkConfig = new DruidConfig(props)
    val settings = DruidSinkSettings(sinkConfig)
    logger.info(
      s"""Settings:
         |$settings
      """.stripMargin)
    writer = Some(new DruidDbWriter(settings))
    enableProgress = sinkConfig.getBoolean(DruidSinkConfigConstants.PROGRESS_COUNTER_ENABLED)
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
      writer.foreach(w => w.write(records.toSeq))
    }

    if (enableProgress) {
      progressCounter.update(records.toVector)
    }
  }

  /**
    * Clean up Druid connections
    **/
  override def stop(): Unit = {
    logger.info("Stopping Druid sink.")
    //writer.foreach(w => w.close())
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    //TODO
    //have the writer expose a is busy; can expose an await using a countdownlatch internally
  }

  override def version: String = manifest.version()

}
