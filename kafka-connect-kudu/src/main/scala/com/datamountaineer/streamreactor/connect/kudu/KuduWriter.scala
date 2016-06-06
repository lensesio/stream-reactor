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

package com.datamountaineer.streamreactor.connect.kudu

import java.util

import com.datamountaineer.streamreactor.connect.KuduConverter
import com.datamountaineer.streamreactor.connect.config.{KuduSetting, KuduSinkConfig}
import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.kududb.client._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try

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

  DbHandler.createTables(setting, client)
  private lazy val kuduTableInserts = DbHandler.buildTableCache(setting, client)
  private lazy val session = client.newSession()
  private val routeMapping = setting.routes
  val autoEvolve = routeMapping.map(r=>(r.getSource, r.isAutoEvolve)).toMap
  private val fields = routeMapping.map({
    rm=>(rm.getSource, rm.getFieldAlias.map({
        fa=>(fa.getField,fa.getAlias)}).toMap)
  }).toMap

  session.isIgnoreAllDuplicateRows

  //initialize error tracker
  initialize(setting.maxRetries, setting.errorPolicy)

  var schemaCache = Map.empty[String, Tuple2[Int, Schema]]

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
      flush()
    }
  }

  /**
    * Per topic, build an new Kudu insert. Per insert build a Kudu row per SinkRecord.
    * Apply the insert per topic for the rows
    * */
  private def applyInsert(topic: String, records: List[SinkRecord], session: KuduSession) = {
    val table = kuduTableInserts.get(topic).get
    logger.debug(s"Preparing write for $topic.")

    val t = Try({
      records
        //.map(r=>handleAlterTable(r, table.getName))
        .map(r=>convert(r, table, fields.get(r.topic()).get))
        .foreach(i=>session.apply(i))

      flush()
    })
    handleTry(t)
    logger.info(s"Written ${records.size} for $topic")
  }


  /**
    * Check alter table is schema has changed
    *
    * @param record The sinkRecord to check the schema for
    * @param tableName The table name to alter
    * */
  private def handleAlterTable(record: SinkRecord, tableName : String) = {
    val version = record.valueSchema().version()
    if (schemaCache.contains(version) && schemaCache.get(record.topic()).get._1 > version) {
      val old = schemaCache.get(record.topic()).get._2
      DbHandler.alterTable(tableName, old, record.valueSchema(), client)
      record
    } else {
      schemaCache.put(record.topic(), Tuple2(version, record.valueSchema()))
      record
    }
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
      val responses = session.flush()

      if (responses != null) {
        responses.asScala.map(r=> {
          //throw and let error policy handle it, don't want to throw RetriableExpection.
          //May want to die if error policy is Throw
          if (r.hasRowError) {
            throw new Throwable("Failed to flush one or more changes. " +
              "Transaction rolled back: " + r.getRowError().toString())
          }
        })
      }
    }
  }
}
