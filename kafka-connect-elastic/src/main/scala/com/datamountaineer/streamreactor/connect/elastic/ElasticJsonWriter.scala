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
import com.datamountaineer.streamreactor.connect.schemas.{ConverterUtil, StructFieldsExtractor}
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.source.Indexable
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import com.sksamuel.elastic4s.BulkResult

import scala.collection.immutable.Iterable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import com.datamountaineer.streamreactor.connect.elastic.indexname.CustomIndexName

class ElasticJsonWriter(client: ElasticClient, settings: ElasticSettings) extends StrictLogging with ConverterUtil {
  logger.info("Initialising Elastic Json writer")

  import ElasticJsonWriter.createIndexName

  val createIndexNameWithSuffix = createIndexName(settings.indexNameSuffix) _

  if (settings.indexAutoCreate) {
    createIndexes()
  }

  implicit object SinkRecordIndexable extends Indexable[SinkRecord] {
    override def json(t: SinkRecord): String = convertValueToJson(t).toString
  }

  /**
    * Create indexes for the topics
    *
    * */
  private def createIndexes() : Unit = {
   settings.tableMap.map({ case(topicName, indexName) => client.execute( {
     settings.documentType match {
       case Some(documentType) => create index createIndexNameWithSuffix(indexName) mappings documentType
       case _ => create index createIndexNameWithSuffix(indexName)
     }
   })})
  }

  /**
    * Close elastic4s client
    * */
  def close() : Unit = client.close()

  private val configMap = settings.routes.map(c => c.getSource -> c).toMap

  /**
    * Write SinkRecords to Elastic Search if list is not empty
    *
    * @param records A list of SinkRecords
    * */
  def write(records: Set[SinkRecord]) : Unit = {
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
    * */
  def insert(records: Map[String, Set[SinkRecord]]) : Iterable[Future[BulkResult]] = {
    val ret = records.map({
      case (topic, sinkRecords) =>
        val fields = settings.fields(topic)
        val ignoreFields = settings.ignoreFields(topic)
        val i = createIndexNameWithSuffix(settings.tableMap(topic))
        val documentType = settings.documentType.getOrElse(i)

        val indexes = sinkRecords
                        .map(r => convert(r, fields, ignoreFields))
                        .map { r =>
                          configMap(r.topic).getWriteMode match {
                            case WriteModeEnum.INSERT => index into i / documentType source r
                            case WriteModeEnum.UPSERT =>
                              // Build a Struct field extractor to get the value from the PK field
                              val pkField = settings.pks(r.topic)
                              // Extractor includes all since we already converted the records to have only needed fields
                              val extractor = StructFieldsExtractor(includeAllFields = true, Map(pkField -> pkField))
                              val fieldsAndValues = extractor.get(r.value.asInstanceOf[Struct]).toMap
                              val pkValue = fieldsAndValues(pkField).toString
                              update id pkValue in i / documentType docAsUpsert fieldsAndValues
                          }
                        }

        val ret = client.execute(bulk(indexes).refresh(true))

        ret.onSuccess({
          case _ => logger.debug(s"Elastic write successful for ${records.size} records!")
        })

        ret.onFailure( {
          case f:Throwable => logger.info(f.toString)
        })
        ret
    })
    ret
  }
}

private object ElasticJsonWriter {
  def createIndexName(maybeIndexNameSuffix: Option[String])(indexName: String): String =
    maybeIndexNameSuffix.fold(indexName) { indexNameSuffix => s"$indexName${CustomIndexName.parseIndexName(indexNameSuffix)}" }
}
