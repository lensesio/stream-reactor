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

package com.datamountaineer.streamreactor.connect.elastic

import com.datamountaineer.streamreactor.connect.elastic.config.ElasticSettings
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.source.Indexable
import com.sksamuel.elastic4s.ElasticClient
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord
import org.elasticsearch.action.bulk.BulkResponse

import scala.collection.immutable.Iterable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.JavaConverters._

class ElasticJsonWriter(client: ElasticClient, settings: ElasticSettings) extends StrictLogging with ConverterUtil {
  logger.info("Initialising Elastic Json writer")

  val routeMappings = settings.routes
  val map = routeMappings.map(rm=>(rm.getSource, rm.getTarget)).toMap

  private val fields = routeMappings.map({
    rm=>(rm.getSource,
      rm.getFieldAlias.asScala.map({
        fa=>(fa.getField,fa.getAlias)
      }).toMap)
  }).toMap

  createIndexes()
  configureConverter(jsonConverter)

  implicit object SinkRecordIndexable extends Indexable[SinkRecord] {
    override def json(t: SinkRecord): String = convertValueToJson(t).toString
  }

  /**
    * Create indexes for the topics
    *
    * */
  private def createIndexes() : Unit = {
   routeMappings.map(s => client.execute( { create index s.getTarget }))
  }

  /**
    * Close elastic4s client
    * */
  def close() : Unit = {
    client.close()
  }

  /**
    * Write SinkRecords to Elastic Search if list is not empty
    *
    * @param records A list of SinkRecords
    * */
  def write(records: List[SinkRecord]) : Unit = {
    if (records.isEmpty) logger.info("No records received.") else {
      val grouped = records.groupBy(_.topic())
      insert(grouped)
    }
  }

  /**
    * Create a bulk index statement and execute against elastic4s client
    *
    * @param records A list of SinkRecords
    * */
  def insert(records: Map[String, List[SinkRecord]]) : Iterable[Future[BulkResponse]] = {
    logger.info(s"Processing ${records.size} records.")

    val ret: Iterable[Future[BulkResponse]] = records.map({
      case (topic, sinkRecords) => {
        val extracted = extractFields(sinkRecords)
        val indexes = extracted.map(r => {
          //lookup mapping
          val i = map.get(r.topic()).get
          index into i / i source r
        })
        val ret = client.execute(bulk(indexes).refresh(true))

        ret.onSuccess({
          case s => logger.debug(s"Elastic write successful for ${records.size} records!")
        })

        ret.onFailure( {
          case f:Throwable => logger.info(f.toString)
        })
        ret
      }
    })

    ret
  }

  /**
    * Extract a subset of fields from the sink records
    *
    * @param records A list of records to extract fields from.
    * @return A new list of sink records with the fields.
  **/
  def extractFields(records: List[SinkRecord]) = {
    if (!fields.isEmpty) {
      records.map(r => extractSinkFields(r, fields.get(r.topic()).get))
    } else {
      records
    }
  }
}
