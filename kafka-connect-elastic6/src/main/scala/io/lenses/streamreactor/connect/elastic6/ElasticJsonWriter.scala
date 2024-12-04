/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.elastic6

import cats.implicits.toBifunctorOps

import java.util
import io.lenses.kcql.Kcql
import io.lenses.kcql.WriteModeEnum
import io.lenses.streamreactor.common.converters.FieldConverter
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.common.schemas.ConverterUtil
import io.lenses.streamreactor.connect.elastic6.config.ElasticSettings
import io.lenses.streamreactor.connect.elastic6.indexname.CreateIndex
import com.sksamuel.elastic4s.bulk.BulkCompatibleRequest
import com.sksamuel.elastic4s.delete.DeleteByIdRequest
import io.lenses.sql.Field
import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.Response
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.typesafe.scalalogging.StrictLogging
import io.lenses.json.sql.JacksonJson
import io.lenses.streamreactor.connect.elastic6.NullValueBehavior.NullValueBehavior
import io.lenses.streamreactor.connect.elastic6.config.ElasticConfigConstants.BEHAVIOR_ON_NULL_VALUES_PROPERTY
import org.apache.kafka.connect.sink.SinkRecord

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.Future
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Try

object NullValueBehavior extends Enumeration {
  type NullValueBehavior = Value
  val FAIL, IGNORE, DELETE = Value

  def fromString(s: String): NullValueBehavior =
    values.find(_.toString.toUpperCase == s).getOrElse(IGNORE)
}

@nowarn
class ElasticJsonWriter(client: KElasticClient, settings: ElasticSettings)
    extends ErrorHandler
    with StrictLogging
    with ConverterUtil {

  logger.info("Initialising Elastic Json writer")

  //initialize error tracker
  initialize(settings.taskRetries, settings.errorPolicy)

  //create the index automatically if it was set to do so
  settings.kcqls.filter(_.isAutoCreate).foreach(client.index)

  private val topicKcqlMap = settings.kcqls.groupBy(_.getSource)

  private val kcqlMap = new util.IdentityHashMap[Kcql, KcqlValues]()
  settings.kcqls.foreach { kcql =>
    kcqlMap.put(
      kcql,
      KcqlValues(
        kcql.getFields.asScala.map(FieldConverter.apply).toSeq,
        kcql.getIgnoredFields.asScala.map(FieldConverter.apply).toSeq,
        kcql.getPrimaryKeys.asScala.map { pk =>
          val path = Option(pk.getParentFields).map(_.asScala.toVector).getOrElse(Vector.empty)
          path :+ pk.getName
        }.toSeq,
        NullValueBehavior.fromString(fetchNullValueBehaviorProperty(kcql)),
      ),
    )

  }

  private def fetchNullValueBehaviorProperty(kcql: Kcql) = {
    val nullBehaviorKeyOption =
      kcql.getProperties.asScala.keys.find(k => BEHAVIOR_ON_NULL_VALUES_PROPERTY.equals(k.toLowerCase))

    kcql.getProperties.get(nullBehaviorKeyOption.getOrElse(() => BEHAVIOR_ON_NULL_VALUES_PROPERTY))
  }

  /**
    * Close elastic4s client
    */
  def close(): Unit = client.close()

  /**
    * Write SinkRecords to Elastic Search if list is not empty
    *
    * @param records A list of SinkRecords
    */
  def write(records: Vector[SinkRecord]): Unit =
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.debug(s"Received ${records.size} records.")
      val grouped = records.groupBy(_.topic())
      insert(grouped)
    }

  /**
    * Create a bulk index statement and execute against elastic4s client
    *
    * @param records A list of SinkRecords
    */
  def insert(records: Map[String, Vector[SinkRecord]]): Unit = {
    logger.info(s"Inserting ${records.size} records")
    val fut = records.flatMap {
      case (topic, sinkRecords) =>
        logger.debug(s"Inserting ${sinkRecords.size} records from $topic")
        val kcqls: Seq[Kcql] = topicKcqlMap.getOrElse(
          topic,
          throw new IllegalArgumentException(
            s"$topic hasn't been configured in KCQL. Configured topics is ${topicKcqlMap.keys.mkString(",")}",
          ),
        )
        kcqls.flatMap { kcql: Kcql =>
          val kcqlValue: KcqlValues = kcqlMap.get(kcql)
          sinkRecords.grouped(settings.batchSize)
            .map { batch =>
              batch.flatMap { r =>
                processRecord(topic, kcql, kcqlValue, r)
              }
            }
            .filter(_.nonEmpty)
            .map { indexes =>
              client.execute(bulk(indexes))
            }
        }
    }

    handleResponse(fut)
  }

  private def handleTombstone(
    topic:        String,
    kcqlValue:    KcqlValues,
    r:            SinkRecord,
    i:            String,
    idFromPk:     String,
    documentType: String,
  ): Option[DeleteByIdRequest] =
    kcqlValue.behaviorOnNullValues match {
      case NullValueBehavior.DELETE =>
        val identifier = if (idFromPk.isEmpty) autoGenId(r) else idFromPk
        logger.debug(
          s"Deleting tombstone record: ${r.topic()} ${r.kafkaPartition()} ${r.kafkaOffset()}. Index: $i, Identifier: $identifier",
        )
        Some(deleteById(new Index(i), documentType, identifier))

      case NullValueBehavior.FAIL =>
        logger.error(
          s"Tombstone record received ${r.topic()} ${r.kafkaPartition()} ${r.kafkaOffset()}. $topic KCQL mapping is configured to fail on tombstone records.",
        )
        throw new IllegalStateException(
          s"$topic KCQL mapping is configured to fail on tombstone records.",
        )

      case NullValueBehavior.IGNORE =>
        logger.info(
          s"Ignoring tombstone record received. for ${r.topic()} ${r.kafkaPartition()} ${r.kafkaOffset()}.",
        )
        None
    }

  private def processRecord(
    topic:     String,
    kcql:      Kcql,
    kcqlValue: KcqlValues,
    r:         SinkRecord,
  ): Option[BulkCompatibleRequest] = {
    val i            = CreateIndex.getIndexName(kcql, r).leftMap(throw _).merge
    val documentType = Option(kcql.getDocType).getOrElse(i)
    val (json, pks) = if (kcqlValue.primaryKeysPath.isEmpty) {
      (Transform(kcqlValue.fields, r.valueSchema(), r.value(), kcql.hasRetainStructure), Seq.empty)
    } else {
      TransformAndExtractPK(kcqlValue,
                            r.valueSchema(),
                            r.value(),
                            kcql.hasRetainStructure,
                            r.keySchema(),
                            r.key(),
                            r.headers(),
      )
    }
    val idFromPk = pks.mkString(settings.pkJoinerSeparator)

    json.filterNot(_.isEmpty) match {
      case Some(value) =>
        kcql.getWriteMode match {
          case WriteModeEnum.INSERT =>
            Some(
              indexInto(i / documentType)
                .id(if (idFromPk.isEmpty) autoGenId(r) else idFromPk)
                .pipeline(kcql.getPipeline)
                .source(value.toString),
            )

          case WriteModeEnum.UPSERT =>
            require(pks.nonEmpty, "Error extracting primary keys")
            Some(update(idFromPk)
              .in(i / documentType)
              .docAsUpsert(value.toString))
        }
      case None =>
        handleTombstone(topic, kcqlValue, r, i, idFromPk, documentType)
    }
  }

  private def handleResponse(fut: immutable.Iterable[Future[Response[BulkResponse]]]): Unit = {
    handleTry(
      Try {
        val result: immutable.Iterable[Response[BulkResponse]] =
          Await.result(Future.sequence(fut), settings.writeTimeout.seconds)
        val errors = result.filter(_.isError).map(_.error)
        if (errors.nonEmpty) {
          logger.error(s"Error writing to Elastic Search: ${JacksonJson.asJson(errors)}")
          throw new RuntimeException(s"Error writing to Elastic Search: ${errors.map(_.reason)}")
        }
        logger.info(
          s"Inserted ${result.size} records. ${result.map { r =>
            s"Items: ${r.result.items.size} took ${r.result.took}ms."
          }.mkString(",")}",
        )
        result
      },
    )
    ()
  }

  /**
    * Create id from record infos
    *
    * @param record One SinkRecord
    */
  def autoGenId(record: SinkRecord): String = {
    val pks: Seq[Any] = Seq(record.topic(), record.kafkaPartition(), record.kafkaOffset())
    pks.mkString(settings.pkJoinerSeparator)
  }

}
case class KcqlValues(
  fields:               Seq[Field],
  ignoredFields:        Seq[Field],
  primaryKeysPath:      Seq[Vector[String]],
  behaviorOnNullValues: NullValueBehavior,
)
