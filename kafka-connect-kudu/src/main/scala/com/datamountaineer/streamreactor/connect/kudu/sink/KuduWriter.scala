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

package com.datamountaineer.streamreactor.connect.kudu.sink

import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.kudu.KuduConverter
import com.datamountaineer.streamreactor.connect.kudu.config.{KuduSettings, KuduSinkConfig, KuduSinkConfigConstants}
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.kududb.client._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class SchemaMap(version: Int, schema: Schema)

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */
object KuduWriter extends StrictLogging {
  def apply(config: KuduSinkConfig, settings: KuduSettings): KuduWriter = {
    val kuduMaster = config.getString(KuduSinkConfigConstants.KUDU_MASTER)
    logger.info(s"Connecting to Kudu Master at $kuduMaster")
    lazy val client = new KuduClient.KuduClientBuilder(kuduMaster).build()
    new KuduWriter(client, settings)
  }
}

class KuduWriter(client: KuduClient, setting: KuduSettings) extends StrictLogging with KuduConverter
  with ErrorHandler with ConverterUtil {
  logger.info("Initialising Kudu writer")

  //pre create tables
  Try(DbHandler.createTables(setting, client)) match {
    case Success(_) =>
    case Failure(f) => logger.warn("Unable to create tables at startup! Tables will be created on delivery of the first record", f)
  }
  //cache tables
  private lazy val kuduTablesCache = collection.mutable.Map(DbHandler.buildTableCache(setting, client).toSeq: _*)
  private lazy val session = client.newSession()

  //ignore duplicate in case of redelivery
  session.isIgnoreAllDuplicateRows

  //initialize error tracker
  initialize(setting.maxRetries, setting.errorPolicy)

  //schema cache
  val schemaCache: mutable.Map[String, SchemaMap] = mutable.Map.empty[String, SchemaMap]

  /**
    * Write SinkRecords to Kudu
    *
    * @param records A list of SinkRecords to write
    **/
  def write(records: Set[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.debug(s"Received ${records.size} records.")

      //if error occurred rebuild cache in case of change on target tables
      if (errored()) {
        kuduTablesCache.empty
        DbHandler.buildTableCache(setting, client)
          .map({ case (topic, table) => kuduTablesCache.put(topic, table) })
      }
      applyInsert(records, session)
    }
  }

  /**
    * Per topic, build an new Kudu insert. Per insert build a Kudu row per SinkRecord.
    * Apply the insert per topic for the rows
    **/
  private def applyInsert(records: Set[SinkRecord], session: KuduSession) = {
    val t = Try({
      records
        .map(r => convert(r, setting.fieldsMap(r.topic), setting.ignoreFields(r.topic)))
        .map(r => applyDDLs(r))
        .map(r => convertToKuduUpsert(r, kuduTablesCache(r.topic)))
        .foreach(i => session.apply(i))
      flush()
    })
    handleTry(t)
    logger.debug(s"Written ${records.size}")
  }

  /**
    * Create the Kudu table if not already done and alter table if required
    *
    * @param record The sink record to create a table for
    * @return A KuduTable
    **/
  private def applyDDLs(record: SinkRecord): SinkRecord = {
    if (!kuduTablesCache.contains(record.topic())) {
      val mapping = setting.routes.filter(f => f.getSource.equals(record.topic())).head
      val table = DbHandler.createTableFromSinkRecord(mapping, record.valueSchema(), client).get
      logger.info(s"Adding table ${mapping.getTarget} to the table cache")
      kuduTablesCache.put(mapping.getSource, table)
    } else {
      handleAlterTable(record)
    }
    record
  }


  /**
    * Check alter table is schema has changed
    *
    * @param record The sinkRecord to check the schema for
    **/
  def handleAlterTable(record: SinkRecord): SinkRecord = {
    val topic = record.topic()
    val allowEvo = setting.allowAutoEvolve.getOrElse(topic, false)

    if (allowEvo) {
      val schema = record.valueSchema()
      val version = schema.version()
      val table = setting.topicTables(topic)
      val cachedSchema = schemaCache.getOrElse(topic, SchemaMap(version, schema))

      //allow evolution
      val evolving = cachedSchema.version < version

      //if table is allowed to evolve all the table
      if (evolving) {
        logger.info(s"Schema change detected for $topic mapped to table $table. Old schema version " +
          s"${cachedSchema.version} new version $version")
        val kuduTable = DbHandler.alterTable(table, cachedSchema.schema, schema, client)
        kuduTablesCache.update(topic, kuduTable)
        schemaCache.update(topic, SchemaMap(version, schema))
      } else {
        schemaCache.update(topic, SchemaMap(version, schema))
      }
    }
    record
  }

  /**
    * Close the Kudu session and client
    **/
  def close(): Unit = {
    logger.info("Closing Kudu Session and Client")
    flush()
    if (!session.isClosed) session.close()
    client.shutdown()
  }

  /**
    * Force the session to flush it's buffers.
    *
    **/
  def flush(): Unit = {
    if (!session.isClosed) {

      //throw and let error policy handle it, don't want to throw RetriableException.
      //May want to die if error policy is Throw
      val flush = session.flush()
      if (flush != null) {
        val errors = flush.asScala
          .flatMap(r => Option(r))
          .withFilter(_.hasRowError)
          .map(_.getRowError.toString)
          .mkString(";")

        if (errors.nonEmpty) {
          throw new RuntimeException(s"Failed to flush one or more changes:$errors")
        }
      }
    }
  }
}
