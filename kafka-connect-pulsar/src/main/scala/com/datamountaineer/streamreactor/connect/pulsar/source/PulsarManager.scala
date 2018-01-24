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
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.concurrent.ExecutorExtension._
import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.datamountaineer.streamreactor.connect.pulsar.ConsumerConfigFactory
import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.apache.pulsar.client.api.{Message, PulsarClient}

import scala.util.{Failure, Success, Try}


class PulsarManager(client: PulsarClient,
                    name: String,
                    kcql: Set[Kcql],
                    messageConverter: PulsarMessageConverter) extends AutoCloseable with StrictLogging {
  private val executor = Executors.newFixedThreadPool(1)

  val configs = ConsumerConfigFactory(name, kcql)

  // Once the consumer is created, it can be used for the entire application life-cycle
  private val consumersMap = kcql.map(c => c.getSource -> client.subscribe(c.getSource, name, configs(c.getSource))).toMap

  @volatile private var stop = false

  executor.submit {
    consumerMessages()
  }
  executor.shutdown()

  private def consumerMessages(): Unit = {
    while (!stop) {
      consumersMap.foreach { case (pulsarTopic, consumer) =>
        val msg = consumer.receive(1000, TimeUnit.MILLISECONDS)

        //TODO: this needs to be implemented to insure exactly once. it require two rounds of poll
        // Acknowledge processing of message so that it can be deleted
        if (messageConverter.convertMessages(msg, pulsarTopic)) consumer.acknowledge(msg)
      }
    }
  }

  override def close(): Unit = {
    stop = true
    executor.shutdownNow()
    Try(executor.awaitTermination(5000, TimeUnit.MILLISECONDS))
    client.close()
  }

  def getRecords(target: util.ArrayList[SourceRecord]) = messageConverter.getRecords(target)
}

case class PulsarMessageConverter(convertersMap: Map[String, Converter],
                                  kcql: Set[Kcql],
                                  throwOnErrors: Boolean,
                                  pollingTimeout: Int,
                                  batchSize: Int) extends StrictLogging {

  private val sourceToTopicMap = kcql.map(c => c.getSource -> c).toMap
  require(kcql.nonEmpty, s"Invalid $kcql parameter. At least one statement needs to be provided")

  private val queue = new LinkedBlockingQueue[SourceRecord]()

  def convertMessages(msg: Message, pulsarTopic: String) = {
    if (msg != null) {
      val matched = sourceToTopicMap
        .filter(t => compareTopic(pulsarTopic, t._1))
        .map(t => t._2.getSource)

      val wildcard = matched.head
      val kafkaTopic = sourceToTopicMap
        .getOrElse(wildcard, throw new ConfigException(s"Topic $pulsarTopic is not configured. Available topics are:${sourceToTopicMap.keySet.mkString(",")}"))
        .getTarget

      val converter = convertersMap.getOrElse(wildcard, throw new RuntimeException(s"$wildcard topic is missing the converter instance."))
      val messageId = Try(msg.getMessageId) match {
        case Success(s) =>
          s.toString

        case Failure(_) =>
          ""
      }

      Option(converter.convert(kafkaTopic, pulsarTopic, messageId, msg.getData)) match {
        case Some(record) =>
          // add the key
          val newRecord = record.newRecord(kafkaTopic, null, Schema.STRING_SCHEMA, msg.getKey, record.valueSchema(), record.value(), System.currentTimeMillis())
          queue.add(newRecord)
          true
        case None =>
          logger.warn(s"Error converting message with id:${msg.getMessageId} on topic:$pulsarTopic. 'null' record returned by converter")
          if (throwOnErrors)
            throw new RuntimeException(s"Error converting message with id:${msg.getMessageId} on topic:$pulsarTopic. 'null' record returned by converter")
          false
      }
    } else {
      false
    }
  }

  def compareTopic(actualTopic: String, subscribedTopic: String): Boolean = {
    actualTopic.matches(subscribedTopic.replaceAll("\\+", "[^/]+").replaceAll("#", ".+"))
  }

  def getRecords(target: util.ArrayList[SourceRecord]): Unit = {
    QueueHelpers.drainWithTimeoutNoGauva(target, batchSize, pollingTimeout, queue)
  }
}
