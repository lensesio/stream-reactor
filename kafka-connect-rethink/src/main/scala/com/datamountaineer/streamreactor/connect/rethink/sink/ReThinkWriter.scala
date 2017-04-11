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

package com.datamountaineer.streamreactor.connect.rethink.sink

import com.datamountaineer.streamreactor.connect.errors.{ErrorHandler, ErrorPolicyEnum}
import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkSinkConfig, ReThinkSinkConfigConstants, ReThinkSinkSetting, ReThinkSinkSettings}
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.rethinkdb.RethinkDB
import com.rethinkdb.net.Connection
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}

import scala.util.{Failure, Try}


object ReThinkWriter extends StrictLogging {
  def apply(config: ReThinkSinkConfig, context: SinkTaskContext): ReThinkWriter = {
    val rethinkHost = config.getString(ReThinkSinkConfigConstants.RETHINK_HOST)
    val port = config.getInt(ReThinkSinkConfigConstants.RETHINK_PORT)

    //set up the connection to the host
    val settings = ReThinkSinkSettings(config)
    lazy val r = RethinkDB.r
    lazy val conn: Connection = r.connection().hostname(rethinkHost).port(port).connect()

    //if error policy is retry set retry interval
    if (settings.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(settings.retryInterval)
    }

    new ReThinkWriter(r, conn = conn, setting = settings)
  }
}

/** *
  * Handles writes to Rethink
  *
  */
class ReThinkWriter(rethink: RethinkDB, conn: Connection, setting: ReThinkSinkSetting)
  extends StrictLogging with ConverterUtil with ErrorHandler {

  logger.info("Initialising ReThink writer")
  //initialize error tracker
  initialize(setting.maxRetries, setting.errorPolicy)

  /**
    * Write a list of SinkRecords
    * to rethink
    *
    * @param records A list of SinkRecords to write.
    **/
  def write(records: List[SinkRecord]): Unit = {

    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.info(s"Received ${records.size} records.")
      if (!conn.isOpen) conn.reconnect()
      val grouped = records.groupBy(_.topic())//.grouped(setting.batchSize)
      grouped.foreach({ case (topic, entries) => writeRecords(topic, entries) })
    }
  }

  /**
    * Write a list of sink records to Rethink
    *
    * @param topic   The source topic
    * @param records The list of sink records to write.
    **/
  private def writeRecords(topic: String, records: List[SinkRecord]) = {
    logger.debug(s"Handling records for $topic")
    val table = setting.topicTableMap(topic)
    val conflict = setting.conflictPolicy(table)

    val writes = records.map(handleSinkRecord).toArray

    val x: java.util.Map[String, Object] = rethink
      .db(setting.db)
      .table(table)
      .insert(writes)
      .optArg("conflict", conflict.toString.toLowerCase)
      .optArg("return_changes", true)
      .run(conn)

    handleFailure(x)
    logger.info(s"Wrote ${writes.length} to rethink.")
  }

  private def handleSinkRecord(record: SinkRecord): java.util.HashMap[String, Any] = {
    val schema = record.valueSchema()
    val value = record.value()
    val pks = setting.pks(record.topic)

    if (schema == null) {
      //try to take it as string
      value match {
        case _: java.util.Map[_, _] =>
          val extracted = convertSchemalessJson(record, setting.fieldMap(record.topic()), setting.ignoreFields(record.topic()))
          //not ideal; but the implementation is hashmap anyway
          SinkRecordConversion.fromMap(record, extracted.asInstanceOf[java.util.HashMap[String, Any]], pks)
        case _ => sys.error("For schemaless record only String and Map types are supported")
      }
    } else {
      schema.`type`() match {
        case Schema.Type.STRING =>
          val extracted = convertStringSchemaAndJson(record, setting.fieldMap(record.topic()), setting.ignoreFields(record.topic()))
          SinkRecordConversion.fromJson(record, extracted, pks)

        case Schema.Type.STRUCT =>
          val extracted = convert(record, setting.fieldMap(record.topic()), setting.ignoreFields(record.topic()))
          SinkRecordConversion.fromStruct(extracted, pks)
        case other => sys.error(s"$other schema is not supported")
      }
    }
  }

  /**
    * Handle any write failures. If errors > 0 handle error
    * according to the policy
    *
    * @param write The write result changes return from rethink
    **/
  private def handleFailure(write: java.util.Map[String, Object]) = {

    val errors = Try(Option(write.get("errors")).getOrElse("0")).getOrElse(0).toString

    if (!errors.equals("0")) {
      val error = Try(Option(write.get("first_error")).getOrElse("Unknown error")).getOrElse("Unknown error")
      val message = s"Write error occurred. ${error.toString}"
      val failure = Failure({
        new Throwable(message)
      })
      handleTry(failure)
    }
  }

  def close(): Unit = conn.close(true)
}
