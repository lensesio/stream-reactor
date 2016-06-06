/**
  * Copyright 2015 Datamountaineer.
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
  **/

package com.datamountaineeer.streamreactor.connect.rethink.sink

import com.datamountaineeer.streamreactor.connect.rethink.config.{ReThinkProps, ReThinkSetting, ReThinkSettings, ReThinkSinkConfig}
import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.fasterxml.jackson.databind.ObjectMapper
import com.rethinkdb.RethinkDB
import com.rethinkdb.net.Connection
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object ReThinkWriter extends StrictLogging {
  def apply(config: ReThinkSinkConfig, context: SinkTaskContext) = {

    val topics = context.assignment().asScala.map(c=>c.topic()).toList
    logger.info(s"Assigned topics ${topics.mkString(",")}")

    val rProps = ReThinkProps(
      config.getString(ReThinkSinkConfig.RETHINK_DB),
      config.getBoolean(ReThinkSinkConfig.AUTO_CREATE_DB),
      config.getBoolean(ReThinkSinkConfig.AUTO_CREATE_TBLS))

    val rethinkHost = config.getString(ReThinkSinkConfig.RETHINK_HOST)

    //set up the connection to the host
    val settings = ReThinkSettings(config, topics, false)
    val routes = settings.routes
    lazy val r = RethinkDB.r
    lazy val conn: Connection = initialise(r, rProps, routes).hostname(rethinkHost).connect()
    new ReThinkWriter(r = r, conn = conn, props = rProps, settings = settings)
  }

  /**
    * Initialize the RethinkDb connection
    *
    * @param rethink A ReThinkDb connection instance
    * @param rProps The connection properties
    * @param routes Topic to table map
    * @return A rethink connection
    * */
  private def initialise(rethink : RethinkDB, rProps: ReThinkProps, routes: List[Config]) = {
    if (rProps.autoCreateDb) {
      rethink.dbCreate(rProps.dbName)
    }

    if (rProps.autoCreateTbls) {
      routes.map(r => rethink.db(rProps.dbName).tableCreate(r.getTarget))
    }
    rethink.connection()
  }
}

/***
  * Handles writes to Rethink
  *
  */
class ReThinkWriter(r: RethinkDB, conn : Connection, props: ReThinkProps, settings: ReThinkSetting)
  extends StrictLogging with ConverterUtil {

  //configure the converters
  val objectMapper = new ObjectMapper()
  logger.info("Initialising ReThink writer")
  private val routeMappings = settings.routes

  private val routeMapping = settings.routes
  private val fields = routeMapping.map({
    rm=>(rm.getSource,
      rm.getFieldAlias.map({
        fa=>(fa.getField,fa.getAlias)
      }).toMap)
  }).toMap

  private val topicTables = routeMappings.map(rm=>(rm.getSource, rm.getTarget)).toMap
  private val conflictMap = routeMappings.map(rm=>(rm.getSource, rm.getTarget)).toMap

  /**
    * Write a list of SinkRecords
    * to rethink
    *
    * @param records A list of SinkRecords to write.
    * */
  def write(records: List[SinkRecord]) = {

    if (records.isEmpty) {
      logger.info("No records received.")
    } else {
      //Try and reconnect
      logger.info("Records received.")

      if (!conn.isOpen) conn.reconnect()

      //group the records by topic to get a map [string, list[sinkrecords]]
      val grouped = records.groupBy(_.topic())

      //Write the records
      grouped.map({
        case (topic, records) => writeRecords(topic, records)
      })
    }
  }

  /**
    * Convert sink records to json
    *
    * @param records A list of sink records to convert.
    * */
  private def toJson(records: List[SinkRecord]) : List[String] = {
    val extracted = records.map(r => extractSinkFields(r, fields.get(r.topic()).get))
    extracted.map(rec=>objectMapper.writeValueAsString(rec))
  }

  /**
    * Write a list of sink records to Rethink
    *
    * @param topic The source topic
    * @param records The list of sink records to write.
    * */
  private def writeRecords(topic: String, records: List[SinkRecord]) = {
    val json = toJson(records)
    val table = topicTables.get(topic).get
    val conflict  = conflictMap.get(table).get
    val write : Map[String, Object] = r.db(props.dbName)
                                      .table(table)
                                      .insert(json)
                                      .optArg("conflict", conflict.toLowerCase)
                                      .optArg("return_changes", true)
                                      .run(conn)

    val errors = write.get("errors").asInstanceOf[Long]
    if (errors != 0L) handleFailure()
  }

  def handleFailure() = {
  }

  /**
    * Ensure that writes on a given table are written to permanent storage. Queries that specify soft durability
    * do not wait for writes to be committed to disk; a call to sync on a table will not return until all previous
    * writes to the table are completed, guaranteeing the data’s persistence
    * */
  def sync() = {
    topicTables.foreach({
      case (topic, table) => r.table(table).sync().run(conn)
    })
  }

  /**
    * Clean up connections
    * */
  def close() = {
    conn.close(true)
  }
}
