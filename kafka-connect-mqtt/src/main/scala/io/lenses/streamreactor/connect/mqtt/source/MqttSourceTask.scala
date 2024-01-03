/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.mqtt.source

import io.lenses.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.common.utils.ProgressCounter
import io.lenses.streamreactor.connect.converters.source.Converter
import io.lenses.streamreactor.connect.mqtt.config.MqttConfigConstants
import io.lenses.streamreactor.connect.mqtt.config.MqttSourceConfig
import io.lenses.streamreactor.connect.mqtt.config.MqttSourceSettings
import io.lenses.streamreactor.connect.mqtt.connection.MqttClientConnectionFn
import io.lenses.streamreactor.common.utils.AsciiArtPrinter.printAsciiHeader
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask

import java.io.File
import java.util
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class MqttSourceTask extends SourceTask with StrictLogging {
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean             = false
  private var mqttManager:    Option[MqttManager] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  override def start(props: util.Map[String, String]): Unit = {
    printAsciiHeader(manifest, "/mqtt-source-ascii.txt")

    val conf = if (context.configs().isEmpty) props else context.configs()

    val settings = MqttSourceSettings(MqttSourceConfig(conf))

    settings.sslCACertFile.foreach { file =>
      if (!new File(file).exists()) {
        throw new ConfigException(s"${MqttConfigConstants.SSL_CA_CERT_CONFIG} is invalid. Can't locate $file")
      }
    }

    settings.sslCertFile.foreach { file =>
      if (!new File(file).exists()) {
        throw new ConfigException(s"${MqttConfigConstants.SSL_CERT_CONFIG} is invalid. Can't locate $file")
      }
    }

    settings.sslCertKeyFile.foreach { file =>
      if (!new File(file).exists()) {
        throw new ConfigException(s"${MqttConfigConstants.SSL_CERT_KEY_CONFIG} is invalid. Can't locate $file")
      }
    }

    val convertersMap = settings.sourcesToConverters.map {
      case (topic, clazz) =>
        logger.info(s"Creating converter instance for $clazz")
        val converter = Try(Class.forName(clazz).getDeclaredConstructor().newInstance()) match {
          case Success(value) => value.asInstanceOf[Converter]
          case Failure(_) =>
            throw new ConfigException(
              s"Invalid ${MqttConfigConstants.KCQL_CONFIG} is invalid. $clazz should have an empty ctor!",
            )
        }
        converter.initialize(conf.asScala.toMap)
        topic -> converter
    }

    logger.info("Starting Mqtt source...")
    mqttManager    = Some(new MqttManager(MqttClientConnectionFn.apply, convertersMap, settings))
    enableProgress = settings.enableProgress
  }

  /**
    * Get all the messages accumulated so far.
    */
  override def poll(): util.List[SourceRecord] = {

    val records = mqttManager.map { manager =>
      val list = new util.LinkedList[SourceRecord]()
      manager.getRecords(list)
      list
    }.orNull

    if (enableProgress) {
      progressCounter.update(records.asScala.toVector)
    }
    records
  }

  /**
    * Shutdown connections
    */
  override def stop(): Unit = {
    logger.info("Stopping Mqtt source.")
    mqttManager.foreach(_.close())
    progressCounter.empty()
  }

  override def version: String = manifest.version()
}
