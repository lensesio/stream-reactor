package com.datamountaineer.streamreactor.connect.coap.source

import java.util.concurrent.LinkedBlockingQueue

import akka.actor.{Actor, Props}
import com.datamountaineer.streamreactor.connect.coap.DTLSConnectionFn
import com.datamountaineer.streamreactor.connect.coap.configs.{CoapSourceSetting, CoapSourceSettings}
import com.datamountaineer.streamreactor.connect.coap.domain.CoapMessageConverter
import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.source.SourceRecord
import org.eclipse.californium.core.coap.Request
import org.eclipse.californium.core.{CoapClient, CoapHandler, CoapResponse}
import org.eclipse.californium.core.network.CoapEndpoint
import org.eclipse.californium.core.network.config.NetworkConfig

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._

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
  def apply(settings: CoapSourceSettings): Map[String, Props] = {
    settings.settings.map(s => (s.kcql.getSource, Props(new CoapReader(s)))).toMap
  }
}

case class CoapReader(setting: CoapSourceSetting) extends Actor with StrictLogging {
  logger.info("Initialising COAP Reader")
  val client = new CoapClient(setting.uri)
  val buffer = new LinkedBlockingQueue[SourceRecord]
  val handler = new MessageHandler(setting.kcql.getTarget, buffer)
  var observing = false

  //Use DTLS is key stores defined
  if (setting.keyStore.isDefined) {
    logger.info("Creating secure client")
    client.setEndpoint(new CoapEndpoint(DTLSConnectionFn(setting), NetworkConfig.getStandard()))
  }

  //discover and check the requested resources
  val resources = client.discover().asScala.map(r => {
    logger.info(s"Discovered resources ${r.getURI}")
    r.getURI
  })

  //check our resource
  if (!resources.contains(s"/${setting.kcql.getSource()}")) {
    throw new ConfigException(s"Resource ${setting.kcql.getSource()} not discovered on ${setting.uri}!")
  } else {
    client.setURI(s"${setting.uri}/${setting.kcql.getSource}")
  }

  //start observing
  def read() = {
    observing = true
    logger.info(s"Starting observation of resource ${setting.uri}/${setting.kcql.getSource} and writing to ${setting.kcql.getTarget}")
    client.observe(handler)
  }

  override def receive: Receive = {
    case DataRequest => sender() ! QueueHelpers.drainQueue(buffer, buffer.size())
    case StartChangeFeed => Future(read).recoverWith { case t =>
      logger.error("Could not retrieve the source records", t)
      Future.failed(t)
    }
    case StopChangeFeed => {
      observing = false
      client.delete(handler)
    }
    case Discover => sender() ! discover
    case Observing => sender() ! observing
  }

  def discover = {
    client.discover()
  }
}

/**
  * A message handler class convert and add any responses
  * to a blocking queue
  * */
class MessageHandler(topic: String, queue: LinkedBlockingQueue[SourceRecord]) extends CoapHandler with StrictLogging {
  val converter = CoapMessageConverter()

  override def onError(): Unit = {
    logger.warn(s"Message dropped for $topic!")
  }

  override def onLoad(response: CoapResponse): Unit = {
    val records = converter.convert(topic, response.advanced())
    logger.debug(s"Received ${response.advanced().toString} for $topic")
    logger.debug(s"Records in queue ${queue.size()} for $topic")
    queue.put(records)
  }
}
