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

  //get mapping routes
  private val topicTables = setting.routes.map(r=>(r.getSource, r.getTarget)).toMap
  private val autoEvolve = setting.allowAutoEvolve
  private val fields = setting.fieldsMap

  //ignore duplicate in case of redelivery
  session.isIgnoreAllDuplicateRows

  //initialize error tracker
  initialize(setting.maxRetries, setting.errorPolicy)

  val schemaCache = mutable.Map.empty[String, SchemaMap]

  /**
    * Write SinkRecords to Kudu
    *
    * @param records A list of SinkRecords to write
    * */
  def write(records: List[SinkRecord]) : Unit = {

    if (records.isEmpty) {
      logger.info("No records received.")
    } else {
      //group the records by topic to get a map [string, list[sinkrecords]] in batches
      val grouped = records.groupBy(_.topic()).grouped(setting.batchSize)

      //for each group get a new insert, convert and apply
      grouped.foreach(g => g.foreach {case (topic, entries) => applyInsert(topic, entries, session)})
    }
  }

  /**
    * Per topic, build an new Kudu insert. Per insert build a Kudu row per SinkRecord.
    * Apply the insert per topic for the rows
    * */
  private def applyInsert(topic: String, records: List[SinkRecord], session: KuduSession) = {
    logger.debug(s"Preparing write for $topic.")

    val t = Try({
      records
        .map(r=>getKuduTable(r))
        .map(r=>convert(r, fields.get(r.topic()).get)) //extract fields
        .map(r=>handleAlterTable(r)) // Handle an changes in the upstream schema
        .map(r=>convertToKuduInsert(r, kuduTablesCache.get(r.topic()).get)) //convert the event to a kudu insert
        .foreach(i=>session.apply(i)) //apply the insert
      flush() //flush and handle any errors
    })
    handleTry(t)
    logger.info(s"Written ${records.size} for $topic")
  }

  /**
    * Get the Kudu table from the cache or create one
    *
    * @param record The sink record to create a table for
    * @return A KuduTable
    * */
  private def getKuduTable(record: SinkRecord) : SinkRecord = {
    kuduTablesCache.getOrElse(record.topic(), {
      val mapping = setting.routes.filter(f=>f.getSource.equals(record.topic())).head
      val table = DbHandler.createTableFromSinkRecord(mapping, record.valueSchema(), client).get
      logger.info(s"Adding table ${mapping.getTarget} to the table cache")
      kuduTablesCache.put(mapping.getSource, table)
    })
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
    val table = topicTables.get(topic).get
    val old = schemaCache.getOrElse(topic, SchemaMap(version, schema))

    //allow evolution
    val evolving = if (autoEvolve.getOrElse(topic, false)) {
      shouldAlter(cachedVersion = old.version, newVersion = version)
    } else {
      false
    }

    evolving match {
      case true => {
        logger.info(s"Schema change detected for ${topic} mapped to table $table. Old schema version " +
          s"${old.version} new version ${version}")
        val kuduTable = DbHandler.alterTable(table, old.schema, schema, client)
        kuduTablesCache.update(topic, kuduTable)
        schemaCache.update(topic, SchemaMap(version, schema))
      }
      case _ => schemaCache.update(topic, SchemaMap(version, schema))
    }
    record
  }


  /**
    * Checks if the topic of the record has changed schemas
    *
    * @param cachedVersion The cached version
    * @param newVersion The new schema version from the sinkrecord
    * @return A boolean indicating if the schema of the target table should change
    * */
  def shouldAlter(cachedVersion : Int, newVersion: Int) : Boolean = {
    cachedVersion < newVersion
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
    if (!session.isClosed()) {

      //throw and let error policy handle it, don't want to throw RetriableExpection.
      //May want to die if error policy is Throw
      val responses = session.flush().asScala
      responses
        .filter(r=>r.hasRowError)
        .map(e=>throw new Throwable("Failed to flush one or more changes: " + e.getRowError().toString()))
    }
  }
}
