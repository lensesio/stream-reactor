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
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.source.Indexable
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord
import org.elasticsearch.action.bulk.BulkResponse

import scala.collection.immutable.Iterable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ElasticJsonWriter(client: ElasticClient, settings: ElasticSettings) extends StrictLogging with ConverterUtil {
  logger.info("Initialising Elastic Json writer")
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
   settings.tableMap.map({ case(k,v) => client.execute( { create index v })})
  }

  /**
    * Close elastic4s client
    * */
  def close() : Unit = client.close()

  /**
    * Write SinkRecords to Elastic Search if list is not empty
    *
    * @param records A list of SinkRecords
    * */
  def write(records: Set[SinkRecord]) : Unit = {
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.info(s"Received ${records.size} records.")
      val grouped = records.groupBy(_.topic())
      insert(grouped)
    }
  }

  /**
    * Create a bulk index statement and execute against elastic4s client
    *
    * @param records A list of SinkRecords
    * */
  def insert(records: Map[String, Set[SinkRecord]]) : Iterable[Future[BulkResponse]] = {
    val ret = records.map({
      case (topic, sinkRecords) =>
        val fields = settings.fields.get(topic).get
        val ignoreFields = settings.ignoreFields.get(topic).get
        val i = settings.tableMap.get(topic).get

        val indexes = sinkRecords
                        .map(r => convert(r, fields, ignoreFields))
                        .map(r => index into i / i source r)

        val ret = client.execute(bulk(indexes).refresh(true))

        ret.onSuccess({
          case s => logger.debug(s"Elastic write successful for ${records.size} records!")
        })

        ret.onFailure( {
          case f:Throwable => logger.info(f.toString)
        })
        ret
    })
    ret
  }
}
