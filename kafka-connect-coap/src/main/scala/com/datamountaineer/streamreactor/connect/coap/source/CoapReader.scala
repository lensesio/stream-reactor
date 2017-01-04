/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */

package com.datamountaineer.streamreactor.connect.coap.source

import java.util
import java.util.concurrent.LinkedBlockingQueue

import akka.actor.{Actor, Props}
import com.datamountaineer.streamreactor.connect.coap.configs.CoapSetting
import com.datamountaineer.streamreactor.connect.coap.connection.CoapManager
import com.datamountaineer.streamreactor.connect.coap.domain.CoapMessageConverter
import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.eclipse.californium.core.{CoapHandler, CoapObserveRelation, CoapResponse, WebLink}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */

case object DataRequest
case object StartChangeFeed
case object StopChangeFeed
case object Observing
case object Discover

object CoapReader {
  def apply(settings: Set[CoapSetting]): Map[String, Props] = {
    settings.map(s => (s.kcql.getSource, Props(new CoapReader(s)))).toMap
  }
}

case class CoapReader(setting: CoapSetting) extends CoapManager(setting) with Actor {
  logger.info(s"Initialising COAP Reader for ${setting.kcql.getSource}")
  val buffer = new LinkedBlockingQueue[SourceRecord]
  val handler = new MessageHandler(setting.kcql.getSource, setting.kcql.getTarget, buffer)
  var observing = false
  var relation : Option[CoapObserveRelation] = None

  //start observing
  def read = {
    observing = true
    logger.info(s"Starting observation of resource ${setting.uri}/${setting.kcql.getSource} and writing to ${setting.kcql.getTarget}")
    relation = Some(client.observe(handler))
  }

  override def receive: Receive = {
    case DataRequest => sender() ! QueueHelpers.drainQueue(buffer, buffer.size())
    case StartChangeFeed => Future(read).recoverWith { case t =>
      logger.error("Could not retrieve the source records", t)
      Future.failed(t)
    }
    case StopChangeFeed =>
      observing = false
      relation.foreach(r => r.proactiveCancel())
      client.delete(handler)
    case Discover => sender() ! discover
    case Observing => sender() ! observing
  }

  def discover: util.Set[WebLink] = client.discover()
}

/**
  * A message handler class convert and add any responses
  * to a blocking queue
  * */
class MessageHandler(resource: String, topic: String, queue: LinkedBlockingQueue[SourceRecord]) extends CoapHandler with StrictLogging {
  val converter = CoapMessageConverter()

  override def onError(): Unit = {
    logger.warn(s"Message dropped for $topic!")
  }

  override def onLoad(response: CoapResponse): Unit = {
    val records = converter.convert(resource, topic, response.advanced())
    logger.debug(s"Received ${response.advanced().toString} for $topic")
    logger.debug(s"Records in queue ${queue.size()} for $topic")
    queue.put(records)
  }
}
