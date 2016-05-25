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

import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.source.Indexable
import com.sksamuel.elastic4s.{ElasticClient, IndexDefinition}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.elasticsearch.action.bulk.BulkResponse
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ElasticJsonWriter(client: ElasticClient, context: SinkTaskContext) extends StrictLogging with ConverterUtil {
  logger.info("Initialising Elastic Json writer")
  val topics = context.assignment().asScala.map(c=>c.topic()).toList
  logger.info(s"Assigned $topics topics.")
  createIndexes(topics)
  configureConverter(jsonConverter)

  implicit object SinkRecordIndexable extends Indexable[SinkRecord] {
    override def json(t: SinkRecord): String = convertValueToJson(t).toString
  }

  /**
    * Create indexes for the topics
    *
    * @param topics A list of topics to create indexes for
    * */
  private def createIndexes(topics: List[String]) : Unit = {
    topics.foreach( t => client.execute( { create index t }))
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
    if (records.isEmpty) logger.info("No records received.") else insert(records)
  }

  /**
    * Create a bulk index statement and execute against elastic4s client
    *
    * @param records A list of SinkRecords
    * */
  def insert(records: List[SinkRecord]) : Future[BulkResponse] = {

    val indexes: List[IndexDefinition] = records.map(r => index into r.topic() / r.topic() source r)
    val ret = client.execute(bulk(indexes).refresh(true))

    ret.onSuccess({
      case s => logger.info(s"Elastic write successful for ${records.size} records!")
    })

    ret.onFailure( {
      case f:Throwable => logger.info(f.toString)
    })
    ret
  }
}
