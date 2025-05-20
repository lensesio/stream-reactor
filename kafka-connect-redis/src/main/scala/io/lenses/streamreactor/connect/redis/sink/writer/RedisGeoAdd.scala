/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.redis.sink.writer

import com.typesafe.scalalogging.StrictLogging
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.config.base.settings.Projections
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.common.schemas.SinkRecordConverterHelper.SinkRecordExtension
import io.lenses.streamreactor.common.schemas.StructHelper
import io.lenses.streamreactor.common.sink.DbWriter
import io.lenses.streamreactor.connect.json.SimpleJsonConverter
import io.lenses.streamreactor.connect.redis.sink.config.RedisKCQLSetting
import io.lenses.streamreactor.connect.redis.sink.config.RedisSinkSettings
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import redis.clients.jedis.Jedis

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.control.Exception.allCatch
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class RedisGeoAdd(sinkSettings: RedisSinkSettings, jedis: Jedis)
    extends DbWriter
    with StrictLogging
    with ErrorHandler
    with GeoAddSupport {
  initialize(sinkSettings.taskRetries, sinkSettings.errorPolicy)

  private lazy val simpleJsonConverter = new SimpleJsonConverter()

  val configs: Set[Kcql] = sinkSettings.kcqlSettings.map(_.kcqlConfig)

  configs.foreach { c =>
    assert(
      c.getSource.trim.nonEmpty,
      "You need to supply a valid source kafka topic to fetch records from. Review your KCQL syntax",
    )
    assert(c.getPrimaryKeys.asScala.nonEmpty,
           "The Redis GeoAdd mode requires at least 1 PK (Primary Key) to be defined",
    )
    assert(c.getStoredAs.equalsIgnoreCase("GeoAdd"), "The Redis GeoAdd mode requires the KCQL syntax: STOREAS GeoAdd")
  }
  private val projections = Projections(
    kcqls            = configs,
    props            = Map.empty,
    errorPolicy      = sinkSettings.errorPolicy,
    errorRetries     = sinkSettings.taskRetries,
    defaultBatchSize = 100,
  )

  // Write a sequence of SinkRecords to Redis
  override def write(records: Seq[SinkRecord]): Unit =
    if (records.isEmpty)
      logger.debug("No records received on 'GeoAdd' Redis writer")
    else {
      logger.debug(s"'GeoAdd' Redis writer received [${records.size}] records")
      insert(records.groupBy(_.topic))
    }

  // Insert a batch of sink records
  def insert(records: Map[String, Seq[SinkRecord]]): Unit =
    records.foreach {
      case (topic, sinkRecords: Seq[SinkRecord]) => {
          val topicSettings: Set[RedisKCQLSetting] = sinkSettings.kcqlSettings.filter(_.kcqlConfig.getSource == topic)
          if (topicSettings.isEmpty)
            logger.warn(s"No KCQL statement set for [$topic]")
          //pass try to error handler and try
          val t = Try {
            sinkRecords.foreach { record =>
              val struct = record.newFilteredRecordAsStruct(projections)
              topicSettings.map { KCQL =>
                val longitudeField = getLongitudeField(KCQL.kcqlConfig)
                val latitudeField  = getLatitudeField(KCQL.kcqlConfig)
                val keys = KCQL.kcqlConfig.getPrimaryKeys.asScala.map(pk =>
                  (pk.toString, pk.toString.replaceAll("\\.", "_")),
                ).toMap

                Try(
                  record.extract(
                    record.value(),
                    record.valueSchema(),
                    keys ++ Map(latitudeField -> latitudeField) ++ Map(longitudeField -> longitudeField),
                    Set.empty,
                  ),
                ) match {
                  case Success(value) =>
                    val helper = StructHelper.StructExtension(value)
                    val pkValue = keys
                      .values
                      .map(k => helper.extractValueFromPath(k))
                      .map {
                        case Right(v) => v.get.toString
                        case Left(e) =>
                          throw new ConnectException(
                            s"Unable to find primary key fields [${keys.mkString(",")}] in record in topic [${record.topic()}], " +
                              s"partition [${record.kafkaPartition()}], offset [${record.kafkaOffset()}], ${e.msg}",
                          )
                      }.mkString(sinkSettings.pkDelimiter)

                    // Use the target (and optionally the prefix) to name the GeoAdd key
                    val optionalPrefix =
                      if (Option(KCQL.kcqlConfig.getTarget).isEmpty) "" else KCQL.kcqlConfig.getTarget.trim
                    val key       = optionalPrefix + pkValue
                    val payload   = simpleJsonConverter.fromConnectData(struct.schema(), struct)
                    val longitude = value.getString(longitudeField)
                    val latitude  = value.getString(latitudeField)

                    if (isDoubleNumber(longitude) && isDoubleNumber(latitude)) {
                      jedis.geoadd(key, longitude.toDouble, latitude.toDouble, payload.toString)
                    } else {
                      logger.warn(
                        s"GeoAdd record contains invalid longitude [$longitude] and latitude [$latitude] values, " +
                          s"Record with key [${record.key}] is skipped",
                      )
                    }

                  case Failure(_) =>
                    throw new ConnectException(
                      s"Failed to constructed new record with primary key fields [${keys.mkString(",")}] and lat and long fields [$longitudeField, $latitudeField]",
                    )
                }
              }
            }
          }
          handleTry(t)
        }
        logger.debug(s"Wrote [${sinkRecords.size}] rows for topic [$topic]")
    }

  def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined

  override def close(): Unit = jedis.close()
}
