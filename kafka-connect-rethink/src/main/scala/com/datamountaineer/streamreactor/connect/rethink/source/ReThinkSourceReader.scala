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

package com.datamountaineer.streamreactor.connect.rethink.source

import java.util
import java.util.concurrent.LinkedBlockingQueue

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
import com.datamountaineer.streamreactor.connect.queues.QueueHelpers._
import com.datamountaineer.streamreactor.connect.rethink.json.Json
import com.rethinkdb.RethinkDB
import com.rethinkdb.gen.ast.Changes
import com.rethinkdb.net.{Connection, Cursor}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 22/09/16. 
  * stream-reactor
  */

class ReThinkSourceReader(rethink : RethinkDB, conn : Connection, route: Config)
  extends StrictLogging with Runnable {

  logger.info("Initialising ReThink reader")
  val keySchema = SchemaBuilder.string().build()
  val valueSchema =  ChangeFeedStructBuilder.schema
  val sourcePartition =  Map.empty[String, String]
  val offset = Map.empty[String, String]
  val queue = new LinkedBlockingQueue[SourceRecord](route.getBatchSize)

  /**
    * Contruct a changefeed, convert any changes
    * to a Struct and add to queue for draining
    * by the task
    *
    * */
  def read() = {
      //create the change feed
      val changes = rethink.table(route.getSource).changes()
      val feed: Cursor[util.HashMap[String, String]] = buildQuery(changes).run(conn)
      while (feed.hasNext()) {
        val value = convert(feed.next().asScala.toMap)
        val record = new SourceRecord(sourcePartition.asJava,
                                      offset.asJava,
                                      route.getTarget,
                                      keySchema,
                                      route.getSource,
                                      valueSchema,
                                      value)
        queue.offer(record)
      }
  }

  /**
    * Drain the queue
    * @return An Sequence of SourceRecords
    * */
  def get(): Seq[SourceRecord] = QueueHelpers.drainQueue(queue, route.getBatchSize).asScala

  /**
    * Build the changefeed query based on the settings
    *
    * @param changes a ReThink Change to apply the options to
    * @return a Change with the required options added
    * */
  def buildQuery(changes: Changes): Changes = {
    changes.optArg("include_states", true)
    changes.optArg("include_initial", true)
    changes.optArg("include_types", true)
    changes
  }

  /**
    * Convert the change feed hashmap to a Struct
    *
    * @param change The HashMap of the changeFeed
    * @return A struct representing the changefeed
    * */
  def convert(change: Map[String, String]) : Struct = ChangeFeedStructBuilder(Json.fromJson[ChangeFeed](change.toString()))

  override def run(): Unit = read()
}
