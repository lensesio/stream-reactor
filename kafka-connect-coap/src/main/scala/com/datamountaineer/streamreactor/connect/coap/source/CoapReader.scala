package com.datamountaineer.streamreactor.connect.coap.source

import java.util.concurrent.LinkedBlockingQueue

import akka.actor.{Actor, Props}
import com.datamountaineer.streamreactor.connect.coap.DTLSConnectionFn
import com.datamountaineer.streamreactor.connect.coap.configs.{CoapSourceSetting, CoapSourceSettings}
import com.datamountaineer.streamreactor.connect.coap.domain.CoapMessageConverter
import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.eclipse.californium.core.{CoapClient, CoapHandler, CoapResponse}
import org.eclipse.californium.core.network.CoapEndpoint
import org.eclipse.californium.core.network.config.NetworkConfig

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */

case object DataRequest
case object StartChangeFeed
case object StopChangeFeed

object CoapReader {
  def apply(settings: CoapSourceSettings): Map[String, Props] = {
    settings.settings.map(s => (s.kcql.getSource, Props(new CoapReader(s)))).toMap
  }
}

case class CoapReader(setting: CoapSourceSetting) extends Actor with StrictLogging {
  logger.info("Initialising COAP Reader")
  val client = new CoapClient(setting.uri + "/" + setting.kcql.getSource)
  val buffer = new LinkedBlockingQueue[SourceRecord]
  val handler = new MessageHandler(setting.kcql.getTarget, buffer)

  if (setting.keyStore.isDefined) {
    logger.info("Creating secure client")
    client.setEndpoint(new CoapEndpoint(DTLSConnectionFn(setting), NetworkConfig.getStandard()))
  }

  def read() = {
    logger.info(s"Starting observation of resource ${setting.kcql.getSource} at ${setting.uri} and writing to ${setting.kcql.getTarget}")
    client.observe(handler)
  }

  override def receive: Receive = {
    case DataRequest => sender() ! QueueHelpers.drainQueue(buffer, buffer.size())
    case StartChangeFeed => Future(read).recoverWith { case t =>
      logger.error("Could not retrieve the source records", t)
      Future.failed(t)
    }
    case StopChangeFeed => client.delete(handler)
  }
}


class MessageHandler(topic: String, queue: LinkedBlockingQueue[SourceRecord]) extends CoapHandler {
  val converter = CoapMessageConverter()

  override def onError(): Unit = ???

  override def onLoad(response: CoapResponse): Unit = {
    val records = converter.convert(topic, response)
    queue.put(records)
  }
}
