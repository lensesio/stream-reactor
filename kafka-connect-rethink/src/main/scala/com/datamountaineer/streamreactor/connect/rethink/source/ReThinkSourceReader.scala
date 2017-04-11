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

package com.datamountaineer.streamreactor.connect.rethink.source

import java.util
import java.util.concurrent.LinkedBlockingQueue

import akka.actor.{Actor, Props}
import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
import com.datamountaineer.streamreactor.connect.rethink.ReThinkConnection
import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkSourceConfig, ReThinkSourceConfigConstants, ReThinkSourceSettings}
import com.datamountaineer.streamreactor.connect.rethink.source.ReThinkSourceReader.{DataRequest, StartChangeFeed, StopChangeFeed}
import com.rethinkdb.RethinkDB
import com.rethinkdb.net.{Connection, Cursor}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by andrew@datamountaineer.com on 22/09/16. 
  * stream-reactor
  */


object ReThinkSourceReader {

  case object DataRequest
  case object StartChangeFeed
  case object StopChangeFeed

  def apply(config: ReThinkSourceConfig, r: RethinkDB): Map[String, Props] = {
    val host = config.getString(ReThinkSourceConfigConstants.RETHINK_HOST)
    val port = config.getInt(ReThinkSourceConfigConstants.RETHINK_PORT)
    val conn = Some(ReThinkConnection(host, port, r))
    val settings = ReThinkSourceSettings(config)
    settings.routes.map(route => (route.getSource, Props(new ReThinkSourceReader(r, conn.get, settings.db, route)))).toMap
  }
}

class ReThinkSourceReader(rethink : RethinkDB, conn : Connection,
                          db: String,
                          route: Config)
  extends Actor with StrictLogging {

  logger.info(s"Initialising ReThink Reader Actor for ${route.getSource}")
  private val keySchema = SchemaBuilder.string().optional().build()
  private val valueSchema =  ChangeFeedStructBuilder.schema
  private val sourcePartition =  Map.empty[String, String]
  private val offset = Map.empty[String, String]
  private var readFlag = true
  private val buffer = new LinkedBlockingQueue[SourceRecord]()

  /**
    * Construct a changefeed, convert any changes
    * to a Struct and add to queue for draining
    * by the task
    *
    * */
  private def read = {
    logger.info(s"Starting changefeed for ${route.getSource}")
    val feed = changeFeed()
    while (feed.hasNext() && readFlag) {
        buffer.offer(convert(feed.next().asScala.toMap))
    }
    feed.close()
    logger.info(s"Stopping changefeed for ${route.getSource}")
  }

  private def changeFeed() : Cursor[util.HashMap[String, String]] = {
    rethink
      .db(db)
      .table(route.getSource)
      .changes()
      .optArg("include_states", true)
      .optArg("include_initial", if (route.isInitialize) true else false)
      .optArg("include_types", true)
      .run(conn)
  }

  private def convert(feed: Map[String, String]) = {
    new SourceRecord(sourcePartition.asJava, offset.asJava, route.getTarget, keySchema, route.getSource, valueSchema,
      ChangeFeedStructBuilder(feed))
  }

  override def receive: Receive = {

    case DataRequest => sender() ! QueueHelpers.drainQueue(buffer, buffer.size())
    case StartChangeFeed => Future(read).recoverWith { case t =>
      logger.error("Could not retrieve the source records", t)
      Future.failed(t)
    }
    case StopChangeFeed =>
      readFlag = false
      conn.close()
  }
}
