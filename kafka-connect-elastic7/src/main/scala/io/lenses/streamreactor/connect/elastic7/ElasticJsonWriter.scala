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
package io.lenses.streamreactor.connect.elastic7

import cats.implicits.toBifunctorOps

import com.fasterxml.jackson.databind.JsonNode
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.Indexable
import com.typesafe.scalalogging.StrictLogging
import io.lenses.kcql.Kcql
import io.lenses.kcql.WriteModeEnum
import io.lenses.sql.Field
import io.lenses.streamreactor.common.converters.FieldConverter
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.common.schemas.ConverterUtil
import io.lenses.streamreactor.connect.elastic7.NullValueBehavior.NullValueBehavior
import io.lenses.streamreactor.connect.elastic7.config.ElasticConfigConstants.BEHAVIOR_ON_NULL_VALUES_PROPERTY
import io.lenses.streamreactor.connect.elastic7.config.ElasticSettings
import io.lenses.streamreactor.connect.elastic7.indexname.CreateIndex
import org.apache.kafka.connect.sink.SinkRecord

import java.util
import scala.annotation.nowarn
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
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

  implicit object SinkRecordIndexable extends Indexable[SinkRecord] {
    override def json(t: SinkRecord): String = convertValueToJson(t).toString
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
  private def insert(records: Map[String, Vector[SinkRecord]]): Unit = {
    val fut = records.flatMap {
      case (topic, sinkRecords) =>
        val kcqls = topicKcqlMap.getOrElse(
          topic,
          throw new IllegalArgumentException(
            s"$topic hasn't been configured in KCQL. Configured topics is ${topicKcqlMap.keys.mkString(",")}",
          ),
        )

        //we might have multiple inserts from the same Kafka Message
        kcqls.flatMap { kcql =>
          val kcqlValue = kcqlMap.get(kcql)
          sinkRecords.grouped(settings.batchSize)
            .map { batch =>
              val indexes = batch.flatMap { r =>
                val i = CreateIndex.getIndexName(kcql, r).leftMap(throw _).merge
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

                if (json.isEmpty || json.get.isEmpty) {
                  kcqlValue.behaviorOnNullValues match {
                    case NullValueBehavior.DELETE =>
                      Option.apply(deleteById(new Index(i), if (idFromPk.isEmpty) autoGenId(r) else idFromPk))

                    case NullValueBehavior.FAIL =>
                      throw new IllegalStateException(
                        s"$topic KCQL mapping is configured to fail on null value, yet it occurred.",
                      )

                    case NullValueBehavior.IGNORE =>
                      return None
                  }

                } else {
                  kcql.getWriteMode match {
                    case WriteModeEnum.INSERT =>
                      Some(
                        indexInto(new Index(i))
                          .id(if (idFromPk.isEmpty) autoGenId(r) else idFromPk)
                          .pipeline(kcql.getPipeline)
                          .source(json.get.toString),
                      )

                    case WriteModeEnum.UPSERT =>
                      require(pks.nonEmpty, "Error extracting primary keys")
                      Some(updateById(new Index(i), idFromPk)
                        .docAsUpsert(json.get)(IndexableJsonNode))
                  }
                }

              }

              client.execute(bulk(indexes).refreshImmediately)
            }
        }
    }

    handleTry(
      Try(
        Await.result(Future.sequence(fut), settings.writeTimeout.seconds),
      ),
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

case object IndexableJsonNode extends Indexable[JsonNode] {
  override def json(t: JsonNode): String = t.toString
}
