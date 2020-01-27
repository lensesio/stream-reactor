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
import java.util.concurrent.TimeUnit
import java.util.Base64

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.datamountaineer.streamreactor.connect.mqtt.config.MqttSourceSettings
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.source.SourceRecord
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import scala.collection.JavaConversions._

class MqttManager(connectionFn: MqttSourceSettings => MqttConnectOptions,
                  convertersMap: Map[String, Converter],
                  settings: MqttSourceSettings) extends AutoCloseable with StrictLogging with MqttCallbackExtended {
  private val kcqlArray = settings.kcql.map(Kcql.parse)

  // This queue is used in messageArrived() callback of MqttClient, hence instantiation should be prior to MqttClient.
  private val queue = new LinkedBlockingQueue[SourceRecord]()
  private val sourceToTopicMap = kcqlArray.map(c => c.getSource -> c).toMap
  require(kcqlArray.nonEmpty, s"Invalid $kcqlArray parameter. At least one statement needs to be provided")
  private val regexMap = kcqlArray.filter(_.getWithRegex != null).map(k => k -> k.getWithRegex.r).toMap
  private val options = connectionFn(settings)

  private val client = createMqttClient()
  client.setCallback(this)

  logger.info(s"Connecting to ${settings.connection}")
  client.connect(options)
  logger.info(s"Connected to ${settings.connection} as ${settings.clientId}")

  override def close(): Unit = {
    client.disconnect(5000)
    client.close()
  }

  override def deliveryComplete(token: IMqttDeliveryToken): Unit = {}

  private def createMqttClient():MqttClient= {
    val servers = settings.connection.split(',').map(_.trim).filter(_.nonEmpty)
    new MqttClient(servers.head, settings.clientId, new MemoryPersistence())
  }

  private def compareTopic(actualTopic: String, subscribedTopic: String): Boolean = {
    actualTopic.matches(
      subscribedTopic.replaceAll("\\$share/.*?/", "")
        .replaceAll("\\+", "[^/]+")
        .replaceAll("#", ".+")
        .replace("$", ".+"))
  }

  private def checkTopic(topic: String, kcql: Kcql): Boolean = {
    regexMap.get(kcql).map(r => r.pattern.matcher(topic).matches())
      .getOrElse(compareTopic(topic, kcql.getSource))
  }

  override def messageArrived(topic: String, message: MqttMessage): Unit = {
    if(settings.logMessageReceived) {
      val msg = new String(Base64.getEncoder.encode(message.getPayload))
      logger.debug(s"Message received on topic [$topic]. Message id =[${message.getId}] , isDuplicate=${message.isDuplicate}, payload=$msg")
    }

    val matched = sourceToTopicMap
      .filter(t => checkTopic(topic, t._2))
      .map(t => t._2.getSource)

    val wildcard = matched.headOption.getOrElse {
      throw new ConfigException(s"Topic '$topic' can not be matched with a source defined by KCQL.")
    }
    val kcql = sourceToTopicMap
      .getOrElse(wildcard, throw new ConfigException(s"Topic $topic is not configured. Available topics are:${sourceToTopicMap.keySet.mkString(",")}"))

    val kafkaTopic = kcql.getTarget match {
      case "$" => topic.replaceFirst("/", "").replaceAll("/", "_")
      case other => other
    }

    val converter = convertersMap.getOrElse(wildcard, throw new RuntimeException(s"$wildcard topic is missing the converter instance."))
    if (!message.isDuplicate) {
      try {
        val keys = Option(kcql.getWithKeys).map { l =>
          val scalaList: Seq[String] = l
          scalaList
        }.getOrElse(Seq.empty[String])
        Option(converter.convert(kafkaTopic, topic, message.getId.toString, message.getPayload, keys, kcql.getKeyDelimeter)) match {
          case Some(record) =>
            queue.add(record)
            message.setRetained(false)
          //message.setPayload(Array.empty)
          case None =>
            logger.warn(s"Error converting message with id:${message.getId} on topic:$topic. 'null' record returned by converter")
            if (settings.throwOnConversion)
              throw new RuntimeException(s"Error converting message with id:${message.getId} on topic:$topic. 'null' record returned by converter")
        }

      } catch {
        case e: Exception =>
          logger.error(s"Error handling message with id:${message.getId} on topic:$topic", e)
          if (settings.throwOnConversion) throw e
          else logger.warn(s"Error is handled. Message will be lost! Id = ${message.getId} on topic=$topic")
      }
    }
  }

  override def connectionLost(cause: Throwable): Unit = {
    logger.warn("Connection lost. Re-connecting is set to true", cause)
  }

  def getRecords(target: util.Collection[SourceRecord]): Int = {
    Option(queue.poll(settings.pollingTimeout, TimeUnit.MILLISECONDS)) match {
      case Some(x) =>
        target.add(x)
        queue.drainTo(target) + 1
      case None =>
        0
    }
  }

  override def connectComplete(reconnect: Boolean, serverURI: String): Unit = {
    val topic = sourceToTopicMap.keySet.toArray
    val qos = Array.fill(sourceToTopicMap.keySet.size)(settings.mqttQualityOfService)

    if (reconnect)
      logger.warn(s"Reconnected. Resubscribing to topic $topic...")
    client.subscribe(topic, qos)
    if (reconnect)
      logger.warn(s"Resubscribed to topic $topic with QoS $qos")
    else logger.info(s"Subscribed to topic $topic with QoS $qos")
  }
}
