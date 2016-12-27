package com.datamountaineer.streamreactor.connect.coap.source

import java.util
import java.util.concurrent.LinkedBlockingQueue
import java.util.{Timer, TimerTask}

import akka.actor.{ActorRef, ActorSystem}
import com.datamountaineer.streamreactor.connect.coap.configs.{CoapSourceConfig, CoapSourceSettings}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
class CoapSourceTask extends SourceTask with StrictLogging {
  private var readers : Set[ActorRef] = _
  private val timer = new Timer()
  private var counter = mutable.Map.empty[String, Long]
  implicit val system = ActorSystem()

  class LoggerTask extends TimerTask {
    override def run(): Unit = logCounts()
  }

  def logCounts(): mutable.Map[String, Long] = {
    counter.foreach( { case (k,v) => logger.info(s"Delivered $v records for $k.") })
    counter.empty
  }

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/coap-source-ascii.txt")).mkString)
    val config = CoapSourceConfig(props)
    val settings = CoapSourceSettings(config)
    val actorProps = CoapReader(settings)
    readers = actorProps.map({ case (source, prop) => system.actorOf(prop, source) }).toSet
    readers.foreach( _ ! StartChangeFeed)
  }

  override def poll(): util.List[SourceRecord] = {
    val records = readers.flatMap(ActorHelper.askForRecords).toList
    records.foreach(r => counter.put(r.topic() , counter.getOrElse(r.topic(), 0L) + 1L))
    records
  }

  override def stop(): Unit = {
    logger.info("Stopping Coap source and closing connections.")
    readers.foreach(_ ! StopChangeFeed)
    timer.cancel()
    counter.empty
    Await.ready(system.terminate(), 1.minute)
  }

  override def version(): String = "1"
}
