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

package com.datamountaineer.streamreactor.connect.kudu

import com.datamountaineer.connector.config.WriteModeEnum
import com.datamountaineer.streamreactor.connect.KuduConverter
import com.datamountaineer.streamreactor.connect.config.{KuduSetting, KuduSinkConfig}
import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.kududb.client._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

case class SchemaMap(version: Int, schema: Schema)

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */
object KuduWriter extends StrictLogging {
  def apply(config: KuduSinkConfig, settings: KuduSetting)  : KuduWriter = {
    val kuduMaster = config.getString(KuduSinkConfig.KUDU_MASTER)
    logger.info(s"Connecting to Kudu Master at $kuduMaster")
    lazy val client = new KuduClient.KuduClientBuilder(kuduMaster).build()
    new KuduWriter(client, settings)
  }
}

class KuduWriter(client: KuduClient, setting: KuduSetting) extends StrictLogging with KuduConverter with ErrorHandler {
  logger.info("Initialising Kudu writer")

  //pre create tables
  DbHandler.createTables(setting, client)
  //cache tables
  private lazy val kuduTablesCache = collection.mutable.Map(DbHandler.buildTableCache(setting, client).toSeq:_*)
  private lazy val session = client.newSession()

  //ignore duplicate in case of redelivery
  session.isIgnoreAllDuplicateRows

  //initialize error tracker
  initialize(setting.maxRetries, setting.errorPolicy)

  //schema cache
  val schemaCache = mutable.Map.empty[String, SchemaMap]

  /**
    * Write SinkRecords to Kudu
    *
    * @param records A list of SinkRecords to write
    * */
  def write(records: Set[SinkRecord]) : Unit = {
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.info(s"Received ${records.size} records.")

      //if error occurred rebuild cache in case of change on target tables
      if (errored()) {
        kuduTablesCache.empty
        DbHandler.buildTableCache(setting, client).map({ case (topic, table) => kuduTablesCache.put(topic, table)})
      }

      applyInsert(records, session)
    }
  }

  /**
    * Per topic, build an new Kudu insert. Per insert build a Kudu row per SinkRecord.
    * Apply the insert per topic for the rows
    * */
  private def applyInsert(records: Set[SinkRecord], session: KuduSession) = {
    val t = Try({
      records
        .map(r => convert(r, setting.fieldsMap.get(r.topic).get, setting.ignoreFields.get(r.topic).get))
        .map(r => applyDDLs(r))
        .map(r => convertToKuduUpsert(r, kuduTablesCache.get(r.topic).get))
        .foreach(i => session.apply(i))
      flush()
    })
    handleTry(t)
    logger.info(s"Written ${records.size}")
  }

  /**
    * Create the Kudu table if not already done and alter table if required
    *
    * @param record The sink record to create a table for
    * @return A KuduTable
    * */
  private def applyDDLs(record: SinkRecord) : SinkRecord = {
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
    * */
  def handleAlterTable(record: SinkRecord) : SinkRecord = {
    val schema = record.valueSchema()
    val version = schema.version()
    val topic = record.topic()
    val table = setting.topicTables.get(topic).get
    val cachedSchema = schemaCache.getOrElse(topic, SchemaMap(version, schema))

    //allow evolution
    val evolving = if (setting.allowAutoEvolve.getOrElse(topic, false)) {
      cachedSchema.version < version
    } else {
      false
    }

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
    record
  }

  /**
    * Close the Kudu session and client
    * */
  def close() : Unit = {
    logger.info("Closing Kudu Session and Client")
    flush()
    if (!session.isClosed) session.close()
    client.shutdown()
  }

  /**
    * Force the session to flush it's buffers.
    *
    * */
  def flush() : Unit = {
    if (!session.isClosed) {

      //throw and let error policy handle it, don't want to throw RetriableExpection.
      //May want to die if error policy is Throw
      val responses = session.flush().asScala
      responses
        .filter(r=>r.hasRowError)
        .map(e=>throw new Throwable("Failed to flush one or more changes: " + e.getRowError.toString))
    }
  }
}
