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
import java.util.concurrent.atomic.AtomicBoolean

import com.datamountaineer.streamreactor.connect.rethink.ReThinkConnection
import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkConfigConstants, ReThinkSourceConfig, ReThinkSourceSetting, ReThinkSourceSettings}
import com.rethinkdb.RethinkDB
import com.rethinkdb.net.{Connection, Cursor}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 22/09/16. 
  * stream-reactor
  */


object ReThinkSourceReadersFactory {

  def apply(config: ReThinkSourceConfig, r: RethinkDB): Set[ReThinkSourceReader] = {
    val conn = Some(ReThinkConnection(r, config))
    val settings = ReThinkSourceSettings(config)
    settings.map(s => new ReThinkSourceReader(r, conn.get, s))
  }
}

class ReThinkSourceReader(rethink: RethinkDB, conn: Connection, setting: ReThinkSourceSetting)
  extends StrictLogging {

  logger.info(s"Initialising ReThink Reader for ${setting.source}")
  private val keySchema = SchemaBuilder.string().optional().build()
  private val valueSchema = ChangeFeedStructBuilder.schema
  private val sourcePartition = Map.empty[String, String]
  private val offset = Map.empty[String, String]
  val feed = changeFeed()
  val records = scala.collection.mutable.MutableList[SourceRecord]()
  val stopFeed = new AtomicBoolean(false)

  /**
    * Construct a change feed, convert any changes
    * to a Struct and add to queue for draining
    * by the task
    *
    **/
  def read() : List[SourceRecord] = {
    records.clear()
    while(feed.hasNext && records.size < setting.batchSize && stopFeed.get().equals(false)) {
      records += convert(feed.next().asScala.toMap)
    }
    records.toList
  }

  private def changeFeed(): Cursor[util.HashMap[String, String]] = {
    logger.info(s"Initialising change feed for ${setting.source}")
    rethink
      .db(setting.db)
      .table(setting.source)
      .changes()
      .optArg("include_states", true)
      .optArg("include_initial", setting.initialise)
      .optArg("include_types", true)
      .run(conn)
  }

  private def convert(feed: Map[String, String]) = {
    new SourceRecord(sourcePartition.asJava, offset.asJava, setting.target, keySchema, setting.source, valueSchema,
      ChangeFeedStructBuilder(feed))
  }

  def stop() = {
    logger.info(s"Closing change feed for ${setting.source}")
    stopFeed.set(true)
    feed.close()
    logger.info(s"Change feed closed for ${setting.source}")
  }
}
