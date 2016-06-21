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

import com.datamountaineeer.streamreactor.connect.rethink.config.{ReThinkSetting, ReThinkSettings, ReThinkSinkConfig}
import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.fasterxml.jackson.databind.ObjectMapper
import com.rethinkdb.RethinkDB
import com.rethinkdb.net.Connection
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}

import scala.collection.JavaConverters._
import scala.util.Try

object ReThinkWriter extends StrictLogging {
  def apply(config: ReThinkSinkConfig, context: SinkTaskContext) = {
    val topics = context.assignment().asScala.map(c=>c.topic()).toList
    logger.info(s"Assigned topics ${topics.mkString(",")}")
    val rethinkHost = config.getString(ReThinkSinkConfig.RETHINK_HOST)

    //set up the connection to the host
    val settings = ReThinkSettings(config, topics, false)
    val routes = settings.routes
    lazy val r = RethinkDB.r
    lazy val conn: Connection = initialiseReThink(r, routes, "").hostname(rethinkHost).connect()
    new ReThinkWriter(r = r, conn = conn, setting = settings)
  }

  /**
    * Initialize the RethinkDb connection
    *
    * @param rethink A ReThinkDb connection instance
    * @param config
    * @return A rethink connection
    * */
  private def initialiseReThink(rethink : RethinkDB, config: List[Config], dbName : String) = {
    config.filter(f=>f.isAutoCreate).map(m=>rethink.db(dbName).tableCreate(m.getTarget))
    rethink.connection()
  }
}

/***
  * Handles writes to Rethink
  *
  */
class ReThinkWriter(r: RethinkDB, conn : Connection, setting: ReThinkSetting)
  extends StrictLogging with ConverterUtil with ErrorHandler {

  //configure the converters
  val objectMapper = new ObjectMapper()
  logger.info("Initialising ReThink writer")
  private val fields = setting.fieldMap
  private val topicTables = setting.topicTableMap
  private val conflictMap = setting.conflictPolicy

  //initialize error tracker
  initialize(setting.maxRetries, setting.errorPolicy)

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
      val grouped = records.groupBy(_.topic()).grouped(setting.batchSize)
      grouped.foreach(g => g.foreach {case (topic, entries) => writeRecords(topic, entries)})
    }
  }

  /**
    * Convert sink records to json
    *
    * @param records A list of sink records to convert.
    * */
  private def toJson(records: List[SinkRecord]) : List[String] = {
    val extracted = records.map(r => convert(r, fields.get(r.topic()).get))
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
    val write : Map[String, Object] = r.db("")
                                      .table(table)
                                      .insert(json)
                                      .optArg("conflict", conflict.toLowerCase)
                                      .optArg("return_changes", true)
                                      .run(conn)

    val errors = write.get("errors").asInstanceOf[Long]
    if (errors != 0L) handleFailure(write)
  }

  def handleFailure(write : Map[String, Object]) = {
    val error = write.get("first_error").asInstanceOf[String]
    handleTry(Try(new Throwable(s"Write error occurred. ${error.toString}")))
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
