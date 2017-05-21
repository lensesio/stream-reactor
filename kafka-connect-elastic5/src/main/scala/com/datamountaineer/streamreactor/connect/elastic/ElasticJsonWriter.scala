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

package com.datamountaineer.streamreactor.connect.elastic

import com.datamountaineer.connector.config.WriteModeEnum
import com.datamountaineer.streamreactor.connect.elastic.config.ElasticSettings
import com.datamountaineer.streamreactor.connect.elastic.indexname.CreateIndex
import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.schemas.{ConverterUtil, StructFieldsExtractor}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{Indexable, TcpClient}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

class ElasticJsonWriter(client: TcpClient, settings: ElasticSettings) extends ErrorHandler with StrictLogging with ConverterUtil {
  logger.info("Initialising Elastic Json writer")

  //initialize error tracker
  initialize(settings.taskRetries, settings.errorPolicy)

  //create the index automatically
  settings.routes.filter(_.isAutoCreate).foreach(kcql => CreateIndex(kcql)(client))

  implicit object SinkRecordIndexable extends Indexable[SinkRecord] {
    override def json(t: SinkRecord): String = convertValueToJson(t).toString
  }

  /**
    * Close elastic4s client
    **/
  def close(): Unit = client.close()

  private val configMap = settings.routes.map(c => c.getSource -> c).toMap

  /**
    * Write SinkRecords to Elastic Search if list is not empty
    *
    * @param records A list of SinkRecords
    **/
  def write(records: Set[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.debug(s"Received ${records.size} records.")
      val grouped = records.groupBy(_.topic())
      insert(grouped)
    }
  }

  /**
    * Create a bulk index statement and execute against elastic4s client
    *
    * @param records A list of SinkRecords
    **/
  def insert(records: Map[String, Set[SinkRecord]]): Unit = {
    val fut = records.map {
      case (topic, sinkRecords) =>
        val fields = settings.fields(topic)
        val ignoreFields = settings.ignoreFields(topic)
        val kcql = configMap.getOrElse(topic, throw new IllegalArgumentException(s"$topic hasn't been configured in KCQL"))
        val i = CreateIndex.getIndexName(kcql)
        val documentType = Option(kcql.getDocType).getOrElse(i)

        val indexes = sinkRecords
          .map(r => convert(r, fields, ignoreFields))
          .map { r =>
            configMap(r.topic).getWriteMode match {
              case WriteModeEnum.INSERT => indexInto(i / documentType).source(r)
              case WriteModeEnum.UPSERT =>
                // Build a Struct field extractor to get the value from the PK field
                val pkField = settings.pks(r.topic)
                // Extractor includes all since we already converted the records to have only needed fields
                val extractor = StructFieldsExtractor(includeAllFields = true, Map(pkField -> pkField))
                val fieldsAndValues = extractor.get(r.value.asInstanceOf[Struct]).toMap
                val pkValue = fieldsAndValues(pkField).toString
                update(pkValue).in(i / documentType).docAsUpsert(fieldsAndValues)
            }
          }

        client.execute(bulk(indexes).refresh(RefreshPolicy.IMMEDIATE))
    }

    handleTry(
      Try(
        Await.result(Future.sequence(fut), settings.writeTimeout.seconds)
      )
    )
  }
}

