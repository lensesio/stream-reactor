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

package com.datamountaineer.streamreactor.connect.mqtt.source

import java.util
import java.util.concurrent.LinkedBlockingQueue

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.source.SourceRecord
import org.eclipse.paho.client.mqttv3._


class MqttManager(connectionFn: (MqttCallback) => MqttClient,
                  convertersMap: Map[String, Converter],
                  qualityOfService: Int,
                  kcql: Array[Config],
                  throwOnErrors: Boolean) extends AutoCloseable with StrictLogging with MqttCallback {
  private val client: MqttClient = connectionFn(this)
  private val sourceToTopicMap = kcql.map(c => c.getSource -> c).toMap
  require(kcql.nonEmpty, s"Invalid $kcql parameter. At least one statement needs to be provided")

  client.subscribe(sourceToTopicMap.keySet.toArray, Array.fill(sourceToTopicMap.keySet.size)(qualityOfService))

  private val queue = new LinkedBlockingQueue[SourceRecord]()

  override def close(): Unit = {
    client.disconnect(5000)
    client.close()
  }

  override def deliveryComplete(token: IMqttDeliveryToken): Unit = {

  }

  override def messageArrived(topic: String, message: MqttMessage): Unit = {
    val kafkaTopic = sourceToTopicMap
      .getOrElse(topic, throw new ConfigException(s"Topic $topic is not configured. Available topics are:${sourceToTopicMap.keySet.mkString(",")}"))
      .getTarget

    val converter = convertersMap.getOrElse(topic, throw new RuntimeException(s"$topic topic is missing the converter instance."))
    if (!message.isDuplicate) {
      try {
        Option(converter.convert(kafkaTopic, topic, message.getId.toString, message.getPayload)) match {
          case Some(record) =>
            queue.add(record)
            message.setRetained(false)
          //message.setPayload(Array.empty)
          case None =>
            logger.warn(s"Error converting message with id:${message.getId} on topic:$topic. 'null' record returned by converter")
            if (throwOnErrors)
              throw new RuntimeException(s"Error converting message with id:${message.getId} on topic:$topic. 'null' record returned by converter")
        }

      } catch {
        case e: Exception =>
          logger.error(s"Error handling message with id:${message.getId} on topic:$topic", e)
          if (throwOnErrors) throw e
          else logger.warn(s"Error is handled. Message will be lost! Id = ${message.getId} on topic=$topic")
      }
    }
  }

  override def connectionLost(cause: Throwable): Unit = {
    logger.warn("Connection lost. Re-connecting is set to true", cause)
  }

  def getRecords(target: util.Collection[SourceRecord]): Int = queue.drainTo(target)
}
