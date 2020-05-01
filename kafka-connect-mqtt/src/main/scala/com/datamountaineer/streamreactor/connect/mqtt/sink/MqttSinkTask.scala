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

package com.datamountaineer.streamreactor.connect.mqtt.sink

import java.util

import com.datamountaineer.streamreactor.connect.converters.sink.Converter
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.mqtt.config.{MqttConfigConstants, MqttSinkConfig, MqttSinkSettings}
import com.datamountaineer.streamreactor.connect.utils.{JarManifest, ProgressCounter}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


/**
  * Created by andrew@datamountaineer.com on 27/08/2017.
  * stream-reactor
  */
class MqttSinkTask extends SinkTask with StrictLogging {
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private var writer: Option[MqttWriter] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/mqtt-sink-ascii.txt")).mkString + s" $version")
    logger.info(manifest.printManifest())

    val conf = if (context.configs().isEmpty) props else context.configs()

    MqttSinkConfig.config.parse(conf)
    val sinkConfig = new MqttSinkConfig(conf)
    enableProgress = sinkConfig.getBoolean(MqttConfigConstants.PROGRESS_COUNTER_ENABLED)
    val settings = MqttSinkSettings(sinkConfig)

    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(sinkConfig.getInt(MqttConfigConstants.ERROR_RETRY_INTERVAL).toLong)
    }

  val convertersMap = settings.sinksToConverters.map { case (topic, clazz) =>
      logger.info(s"Creating converter instance for $clazz and topic $topic")

      if (clazz == null) {
        topic -> null
      } else {
        val converter = Try(Class.forName(clazz).newInstance()) match {
          case Success(value) => value.asInstanceOf[Converter]
          case Failure(_) => throw new ConfigException(s"Invalid ${MqttConfigConstants.KCQL_CONFIG} is invalid. $clazz should have an empty ctor!")
        }
        import scala.collection.JavaConverters._
        converter.initialize(conf.asScala.toMap)
        topic -> converter
      }

    }

    writer = Some(MqttWriter(settings, convertersMap))
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val recs = records.asScala
    writer.foreach(w => w.write(recs.toSet))

    if (enableProgress) {
      progressCounter.update(recs.toVector)
    }
  }

  /**
    * Clean up writer
    **/
  override def stop(): Unit = {
    logger.info("Stopping Mqtt sink.")
    writer.foreach(w => w.close)
    progressCounter.empty
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w => w.flush)
  }

  override def version: String = manifest.version()
}
