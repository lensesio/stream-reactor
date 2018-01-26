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

package com.datamountaineer.streamreactor.connect.elastic6

import java.util

import com.datamountaineer.kcql.{Kcql, WriteModeEnum}
import com.datamountaineer.streamreactor.connect.converters.FieldConverter
import com.datamountaineer.streamreactor.connect.elastic6.config.ElasticSettings
import com.datamountaineer.streamreactor.connect.elastic6.indexname.CreateIndex
import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.fasterxml.jackson.databind.JsonNode
import com.landoop.sql.Field
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Indexable
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

class ElasticJsonWriter(client: KElasticClient, settings: ElasticSettings)
  extends ErrorHandler with StrictLogging with ConverterUtil {

  logger.info("Initialising Elastic Json writer")

  //initialize error tracker
  initialize(settings.taskRetries, settings.errorPolicy)

  //create the index automatically if it was set to do so
  settings.kcqls.filter(_.isAutoCreate).foreach(client.index)

  private val topicKcqlMap = settings.kcqls.groupBy(_.getSource)

  private val kcqlMap = new util.IdentityHashMap[Kcql, KcqlValues]()
  settings.kcqls.foreach { kcql =>
    kcqlMap.put(kcql,
      KcqlValues(
        kcql.getFields.map(FieldConverter.apply),
        kcql.getIgnoredFields.map(FieldConverter.apply),
        kcql.getPrimaryKeys.map { pk =>
          val path = Option(pk.getParentFields).map(_.toVector).getOrElse(Vector.empty)
          path :+ pk.getName
        }
      ))

  }


  implicit object SinkRecordIndexable extends Indexable[SinkRecord] {
    override def json(t: SinkRecord): String = convertValueToJson(t).toString
  }

  /**
    * Close elastic4s client
    **/
  def close(): Unit = client.close()


  /**
    * Write SinkRecords to Elastic Search if list is not empty
    *
    * @param records A list of SinkRecords
    **/
  def write(records: Vector[SinkRecord]): Unit = {
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
  def insert(records: Map[String, Vector[SinkRecord]]): Unit = {
    val fut = records.flatMap {
      case (topic, sinkRecords) =>
        val kcqls = topicKcqlMap.getOrElse(topic, throw new IllegalArgumentException(s"$topic hasn't been configured in KCQL. Configured topics is ${topicKcqlMap.keys.mkString(",")}"))

        //we might have multiple inserts from the same Kafka Message
        kcqls.flatMap { kcql =>
          val i = CreateIndex.getIndexName(kcql)
          val documentType = Option(kcql.getDocType).getOrElse(i)
          val kcqlValue = kcqlMap(kcql)
          sinkRecords.grouped(settings.batchSize)
            .map { batch =>
              val indexes = batch.map { r =>
                val (json, pks) = if(kcqlValue.primaryKeysPath.isEmpty) {
                  (Transform(
                    kcqlValue.fields,
                    kcqlValue.ignoredFields,
                    r.valueSchema(),
                    r.value(),
                    kcql.hasRetainStructure
                  ), Seq.empty)
                } else {
                  TransformAndExtractPK(
                    kcqlValue.fields,
                    kcqlValue.ignoredFields,
                    kcqlValue.primaryKeysPath,
                    r.valueSchema(),
                    r.value(),
                    kcql.hasRetainStructure)
                }
                val idFromPk = pks.mkString(settings.pkJoinerSeparator);

                kcql.getWriteMode match {
                  case WriteModeEnum.INSERT =>
                    indexInto(i / documentType)
                      .pipeline(kcql.getPipeline)
                      .id(if (idFromPk.isEmpty) autoGenId(r) else idFromPk)
                      .source(json.toString)

                  case WriteModeEnum.UPSERT =>
                    require(pks.nonEmpty, "Error extracting primary keys")
                    update(idFromPk)
                      .in(i / documentType)
                      .docAsUpsert(json)(IndexableJsonNode)
                }
              }

              client.execute(bulk(indexes).refreshImmediately)
            }
        }
    }

    handleTry(
      Try(
        Await.result(Future.sequence(fut), settings.writeTimeout.seconds)
      )
    )
  }

  /**
    * Create id from record infos
    *
    * @param record One SinkRecord
    **/
  def autoGenId(record: SinkRecord): String = {
    val pks = Seq(record.topic(), record.kafkaPartition(), record.kafkaOffset())
    pks.mkString(settings.pkJoinerSeparator)
  }

  private case class KcqlValues(fields: Seq[Field],
                                ignoredFields: Seq[Field],
                                primaryKeysPath: Seq[Vector[String]])

}


case object IndexableJsonNode extends Indexable[JsonNode] {
  override def json(t: JsonNode): String = t.toString
}