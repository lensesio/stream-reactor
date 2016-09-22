/**
  * Copyright 2016 Datamountaineer.
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

package com.datamountaineer.streamreactor.connect.rethink.sink

import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkSinkSetting, ReThinkSinkSettings, ReThinkSinkConfig}
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.rethinkdb.RethinkDB
import com.rethinkdb.model.MapObject
import com.rethinkdb.net.Connection
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import scala.util.Failure
import scala.collection.JavaConverters._


object ReThinkWriter extends StrictLogging {
  def apply(config: ReThinkSinkConfig, context: SinkTaskContext) : ReThinkWriter = {
    val rethinkHost = config.getString(ReThinkSinkConfig.RETHINK_HOST)
    val port = config.getInt(ReThinkSinkConfig.RETHINK_PORT)

    //set up the connection to the host
    val settings = ReThinkSinkSettings(config)
    lazy val r = RethinkDB.r
    lazy val conn: Connection = r.connection().hostname(rethinkHost).port(port).connect()
    new ReThinkWriter(r, conn = conn, setting = settings)
  }
}
/***
  * Handles writes to Rethink
  *
  */
class ReThinkWriter(rethink : RethinkDB, conn : Connection, setting: ReThinkSinkSetting)
  extends StrictLogging with ConverterUtil with ErrorHandler {

  logger.info("Initialising ReThink writer")
  //initialize error tracker
  initialize(setting.maxRetries, setting.errorPolicy)

  /**
    * Write a list of SinkRecords
    * to rethink
    *
    * @param records A list of SinkRecords to write.
    * */
  def write(records: List[SinkRecord]) : Unit = {

    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.info(s"Received ${records.size} records.")
      if (!conn.isOpen) conn.reconnect()
      val grouped = records.groupBy(_.topic()).grouped(setting.batchSize)
      grouped.foreach(g => g.foreach {case (topic, entries) => writeRecords(topic, entries)})
    }
  }

  /**
    * Write a list of sink records to Rethink
    *
    * @param topic The source topic
    * @param records The list of sink records to write.
    * */
  private def writeRecords(topic: String, records: List[SinkRecord]) = {
    logger.info(s"Handling records for $topic")
    val table = setting.topicTableMap(topic)
    val conflict  = setting.conflictPolicy(table)
    val pks = setting.pks(topic)

    val writes: Array[MapObject] = records.map(r => {
      val valueSchema = Option(r.valueSchema())
      valueSchema match {
        case Some(_) => {
          val extracted = convert(r, setting.fieldMap(r.topic()), setting.ignoreFields(r.topic()))
          ReThinkSinkConverter.convertToReThink(rethink, extracted, pks)
        }
        case None => ReThinkSinkConverter.convertToReThinkSchemaless(rethink, r)
      }
    } ).toArray

    val x : java.util.Map[String, Object] = rethink
                                              .db(setting.db)
                                              .table(table)
                                              .insert(writes)
                                              .optArg("conflict", conflict.toString.toLowerCase)
                                              .optArg("return_changes", true)
                                              .run(conn)

    handleFailure(x.asScala.toMap)
    logger.info(s"Wrote ${writes.size} to rethink.")
  }

  /**
    * Handle any write failures. If errors > 0 handle error
    * according to the policy
    *
    * @param write The write result changes return from rethink
    * */
  private def handleFailure(write : Map[String, Object]) = {

    val errors = write.getOrElse("errors", 0).toString

    if (!errors.equals("0")) {
      val error = write.getOrElse("first_error","Unknown error")
      val message = s"Write error occurred. ${error.toString}"
      val failure = Failure({new Throwable(message)})
      handleTry(failure)
    }
  }

  def close() : Unit = conn.close(true)
}
