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

package com.datamountaineer.streamreactor.connect.pulsar.source

import java.util

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.datamountaineer.streamreactor.connect.pulsar.config.{PulsarConfigConstants, PulsarSourceConfig, PulsarSourceSettings}
import com.datamountaineer.streamreactor.connect.utils.{JarManifest, ProgressCounter}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.apache.pulsar.client.api.PulsarClient
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class PulsarSourceTask extends SourceTask with StrictLogging {
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private var pulsarManager: Option[PulsarManager] = None
  private val manifest = JarManifest()

  override def start(props: util.Map[String, String]): Unit = {

    logger.info(scala.io.Source.fromInputStream(this.getClass.getResourceAsStream("/pulsar-source-ascii.txt")).mkString + s" v $version")
    logger.info(manifest.printManifest())
    implicit val settings = PulsarSourceSettings(PulsarSourceConfig(props))

    val convertersMap = settings.sourcesToConverters.map { case (topic, clazz) =>
      logger.info(s"Creating converter instance for $clazz")
      val converter = Try(this.getClass.getClassLoader.loadClass(clazz).newInstance()) match {
        case Success(value) => value.asInstanceOf[Converter]
        case Failure(_) => throw new ConfigException(s"Invalid ${PulsarConfigConstants.KCQL_CONFIG} is invalid. $clazz should have an empty ctor!")
      }
      import scala.collection.JavaConverters._
      converter.initialize(props.asScala.toMap)
      topic -> converter
    }
    logger.info("Starting Pulsar source...")
    pulsarManager = Some(new PulsarManager(PulsarClient.create(settings.connection), convertersMap, settings.kcql.map(Kcql.parse), settings.throwOnConversion, settings.pollingTimeout))
    enableProgress = settings.enableProgress
  }

  /**
    * Get all the messages accumulated so far.
    **/
  override def poll(): util.List[SourceRecord] = {

    val records = pulsarManager.map { manager =>
      val list = new util.LinkedList[SourceRecord]()
      manager.getRecords(list)
      list
    }.orNull

    if (enableProgress) {
      progressCounter.update(records.toVector)
    }
    records
  }

  /**
    * Shutdown connections
    **/
  override def stop(): Unit = {
    logger.info("Stopping Pulsar source.")
    pulsarManager.foreach(_.close())
    progressCounter.empty
  }

  override def version: String = manifest.version()
}
