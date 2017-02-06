/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.mqtt.source

import java.io.File
import java.util
import java.util.{Timer, TimerTask}

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.datamountaineer.streamreactor.connect.mqtt.config.{MqttSourceConfig, MqttSourceSettings}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class MqttSourceTask extends SourceTask with StrictLogging {
  private var mqttManager: Option[MqttManager] = None
  private val counter = mutable.Map.empty[String, Long]
  private var timestamp: Long = 0

  class LoggerTask extends TimerTask {
    override def run(): Unit = logCounts()
  }

  def logCounts(): mutable.Map[String, Long] = {
    counter.foreach( { case (k,v) => logger.info(s"Delivered $v records for $k.") })
    counter.empty
  }

  override def start(props: util.Map[String, String]): Unit = {

    logger.info(scala.io.Source.fromInputStream(this.getClass.getResourceAsStream("/mqtt-source-ascii.txt")).mkString)
    implicit val settings = MqttSourceSettings(MqttSourceConfig(props))

    settings.sslCACertFile.foreach { file =>
      if (!new File(file).exists()) {
        throw new ConfigException(s"${MqttSourceConfig.SSL_CA_CERT_CONFIG} is invalid. Can't locate $file")
      }
    }

    settings.sslCertFile.foreach { file =>
      if (!new File(file).exists()) {
        throw new ConfigException(s"${MqttSourceConfig.SSL_CERT_CONFIG} is invalid. Can't locate $file")
      }
    }

    settings.sslCertKeyFile.foreach { file =>
      if (!new File(file).exists()) {
        throw new ConfigException(s"${MqttSourceConfig.SSL_CERT_KEY_CONFIG} is invalid. Can't locate $file")
      }
    }

    val convertersMap = settings.sourcesToConverters.map { case (topic, clazz) =>
      logger.info(s"Creating converter instance for $clazz")
      val converter = Try(this.getClass.getClassLoader.loadClass(clazz).newInstance()) match {
        case Success(value) => value.asInstanceOf[Converter]
        case Failure(_) => throw new ConfigException(s"Invalid ${MqttSourceConfig.CONVERTER_CONFIG} is invalid. $clazz should have an empty ctor!")
      }
      import scala.collection.JavaConverters._
      converter.initialize(props.asScala.toMap)
      topic -> converter
    }
    logger.info("Starting Mqtt source...")
    mqttManager = Some(new MqttManager(MqttClientConnectionFn.apply, convertersMap, settings.mqttQualityOfService, settings.kcql.map(Config.parse), settings.throwOnConversion))

  }

  /**
    * Get all the messages accumulated so far.
    **/
  override def poll(): util.List[SourceRecord] = {

    val records = mqttManager.map { manager =>
      val list = new util.LinkedList[SourceRecord]()
      manager.getRecords(list)
      list
    }.orNull

    records.foreach(r => counter.put(r.topic() , counter.getOrElse(r.topic(), 0L) + 1L))

    val newTimestamp = System.currentTimeMillis()
    if (counter.nonEmpty && scala.concurrent.duration.SECONDS.toSeconds(newTimestamp - timestamp) >= 60) {
      logCounts()
    }
    timestamp = newTimestamp

    records
  }

  /**
    * Shutdown connections
    **/
  override def stop(): Unit = {
    logger.info("Stopping Mqtt source.")
    mqttManager.foreach(_.close())
  }

  override def version(): String = getClass.getPackage.getImplementationVersion
}
