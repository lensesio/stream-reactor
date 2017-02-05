/*
 *  Copyright 2016 Datamountaineer.
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

package com.datamountaineer.streamreactor.connect.rethink.source

import java.util
import java.util.{Timer, TimerTask}

import akka.actor.{ActorRef, ActorSystem}
import com.datamountaineer.streamreactor.connect.rethink.config.ReThinkSourceConfig
import com.datamountaineer.streamreactor.connect.rethink.source.ReThinkSourceReader.{StartChangeFeed, StopChangeFeed}
import com.rethinkdb.RethinkDB
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by andrew@datamountaineer.com on 22/09/16. 
  * stream-reactor
  */
class ReThinkSourceTask extends SourceTask with StrictLogging {
  private var readers : Set[ActorRef] = _
  private var timestamp: Long = 0
  private val counter = mutable.Map.empty[String, Long]
  implicit val system = ActorSystem()

  class LoggerTask extends TimerTask {
    override def run(): Unit = logCounts()
  }

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/rethink-source-ascii.txt")).mkString)
    val config = ReThinkSourceConfig(props)
    lazy val r = RethinkDB.r
    startReaders(config, r)

  }

  def startReaders(config: ReThinkSourceConfig, rethinkDB: RethinkDB): Unit = {
    val actorProps = ReThinkSourceReader(config, rethinkDB)
    readers = actorProps.map({ case (source, prop) => system.actorOf(prop, source) }).toSet
    readers.foreach( _ ! StartChangeFeed)
  }

  def logCounts(): mutable.Map[String, Long] = {
    counter.foreach( { case (k,v) => logger.info(s"Delivered $v records for $k.") })
    counter.empty
  }

  /**
    * Read from readers queue
    * */
  override def poll(): util.List[SourceRecord] = {
   val records = readers.flatMap(ActorHelper.askForRecords).toList
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
    * */
  override def stop(): Unit = {
    logger.info("Stopping ReThink source and closing connections.")
    readers.foreach(_ ! StopChangeFeed)
    counter.empty
    Await.ready(system.terminate(), 1.minute)
  }

  override def version(): String = getClass.getPackage.getImplementationVersion
}
