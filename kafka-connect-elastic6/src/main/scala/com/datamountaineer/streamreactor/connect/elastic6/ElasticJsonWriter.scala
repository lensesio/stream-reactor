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
import com.datamountaineer.streamreactor.common.config.base.settings.Projections
import com.datamountaineer.streamreactor.common.converters.FieldConverter
import com.datamountaineer.streamreactor.common.errors.ErrorHandler
import com.datamountaineer.streamreactor.common.schemas.SinkRecordConverterHelper.SinkRecordExtension
import com.datamountaineer.streamreactor.common.schemas.StructHelper
import com.datamountaineer.streamreactor.connect.elastic6.Transform.simpleJsonConverter
import com.datamountaineer.streamreactor.connect.elastic6.config.{ElasticConfigConstants, ElasticSettings}
import com.datamountaineer.streamreactor.connect.elastic6.indexname.CreateIndex
import com.fasterxml.jackson.databind.JsonNode
import com.landoop.sql.Field
import com.sksamuel.elastic4s.Indexable
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class ElasticJsonWriter(client: KElasticClient, settings: ElasticSettings)
  extends ErrorHandler with StrictLogging {

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
        kcql.getFields.asScala.map(FieldConverter.apply),
        kcql.getIgnoredFields.asScala.map(FieldConverter.apply),
        kcql.getPrimaryKeys.asScala.map { pk =>
          val path = Option(pk.getParentFields).map(_.asScala.toVector).getOrElse(Vector.empty)
          path :+ pk.getName
        }
      ))

  }

  private val projections = Projections(
    kcqls = settings.kcqls.toSet,
    props = Map.empty,
    errorPolicy = settings.errorPolicy,
    errorRetries = settings.taskRetries,
    defaultBatchSize = ElasticConfigConstants.BATCH_SIZE_DEFAULT
  )

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
          val kcqlValue = kcqlMap.get(kcql)
          sinkRecords.grouped(settings.batchSize)
            .map { batch =>
              val indexes = batch.map { r =>

                val struct = r.newFilteredRecordAsStruct(projections)
                val payload = simpleJsonConverter.fromConnectData(struct.schema(), struct)
                val (json, pks) = if (kcqlValue.primaryKeysPath.isEmpty) {
                  (payload, Seq.empty)
                } else {
                  //extract will flatten if parent/child detected
                  val keys = kcql.getPrimaryKeys.asScala.map(pk => (pk.toString, pk.toString.replaceAll("\\.", "_"))).toMap

                  val pkValues = Try(r.extract(
                    r.value(),
                    r.valueSchema(),
                    keys,
                    Set.empty)) match {
                    case Success(value) =>
                      val helper = StructHelper.StructExtension(value)
                      keys
                        .values
                        .map(p => {
                          helper.extractValueFromPath(p) match {
                            case Right(v) => v.get.toString
                            case Right(None) => throw new ConnectException(s"Null values for primary key field values [${keys.mkString(",")}] " +
                              s"in record in topic [${r.topic()}], " +
                              s"partition [${r.kafkaPartition()}], offset [${r.kafkaOffset()}]")
                            case Left(e) => throw new ConnectException(s"Unable to find all primary key field values [${keys.mkString(",")}] " +
                              s"in record in topic [${r.topic()}], " +
                              s"partition [${r.kafkaPartition()}], offset [${r.kafkaOffset()}], ${e.msg}")
                          }
                        })

                    case Failure(exception) =>
                      throw new ConnectException(s"Failed to constructed new record with primary key fields [${keys.mkString(",")}]. ${exception.getMessage}")
                  }
                  (payload, pkValues.toSet)
                }

                val idFromPk = pks.mkString(settings.pkJoinerSeparator)

                kcql.getWriteMode match {
                  case WriteModeEnum.INSERT =>
                    indexInto(i / documentType)
                      .id(if (idFromPk.isEmpty) autoGenId(r) else idFromPk)
                      .pipeline(kcql.getPipeline)
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