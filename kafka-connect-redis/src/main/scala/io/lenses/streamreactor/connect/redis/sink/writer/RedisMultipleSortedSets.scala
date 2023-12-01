/*
 * Copyright 2017-2023 Lenses.io Ltd
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
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * The PK (Primary Key) Redis `writer` stores in 1 Sorted Set / per field value of the PK
  *
  * KCQL syntax requires 1 Primary Key to be defined (plus) STOREAS SortedSet
  *
  * .. PK .. STOREAS SortedSet
  */
class RedisMultipleSortedSets(sinkSettings: RedisSinkSettings, jedis: Jedis)
    extends DbWriter
    with StrictLogging
    with ErrorHandler
    with SortedSetSupport {
  initialize(sinkSettings.taskRetries, sinkSettings.errorPolicy)

  private lazy val simpleJsonConverter = new SimpleJsonConverter()

  val configs: Set[Kcql] = sinkSettings.kcqlSettings.map(_.kcqlConfig)
  configs.foreach { c =>
    assert(
      c.getSource.trim.nonEmpty,
      "You need to supply a valid source kafka topic to fetch records from. Review your KCQL syntax",
    )
    assert(
      c.getPrimaryKeys.asScala.nonEmpty,
      "The Redis MultipleSortedSets mode requires at least 1 PK (Primary Key) to be defined",
    )
    assert(
      c.getStoredAs.equalsIgnoreCase("SortedSet"),
      "The Redis MultipleSortedSets mode requires the KCQL syntax: STOREAS SortedSet",
    )
  }

  // Write a sequence of SinkRecords to Redis
  override def write(records: Seq[SinkRecord]): Unit =
    if (records.isEmpty)
      logger.debug("No records received on MultipleSortedSets SortedSet Redis writer")
    else {
      logger.debug(s"'MultipleSortedSets' Redis writer received [${records.size}] records")
      insert(records.groupBy(_.topic))
    }

  // Insert a batch of sink records
  def insert(records: Map[String, Seq[SinkRecord]]): Unit =
    records.foreach {
      case (topic, sinkRecords: Seq[SinkRecord]) => {
          val topicSettings: Set[RedisKCQLSetting] = sinkSettings.kcqlSettings.filter(_.kcqlConfig.getSource == topic)
          if (topicSettings.isEmpty) {
            throw new ConnectException(s"No KCQL statement set for [$topic]")
          }
          //pass try to error handler and try
          val t = Try {
            sinkRecords.foreach { record =>
              topicSettings.map { KCQL =>
                val keys = KCQL.kcqlConfig.getPrimaryKeys.asScala.map(pk =>
                  (pk.toString, pk.toString.replaceAll("//.", "_")),
                ).toMap
                val fields      = KCQL.kcqlConfig.getFields.asScala.map(f => f.toString -> f.getAlias).toMap
                val scoreField  = getScoreField(KCQL.kcqlConfig)
                val scoreFields = Map(scoreField -> scoreField)

                //convert with fields and score
                val payload =
                  Try(record.extract(record.value(), record.valueSchema(), fields ++ scoreFields, Set.empty)) match {
                    case Success(value) => simpleJsonConverter.fromConnectData(value.schema(), value)
                    case Failure(_) =>
                      throw new ConnectException(
                        s"Failed to constructed new record with fields [${fields.mkString(",")}] and score fields [${scoreFields.mkString(",")}]",
                      )
                  }

                //extract pk, score fields and send
                Try(record.extract(record.value(), record.valueSchema(), keys ++ scoreFields, Set.empty)) match {
                  case Success(value) =>
                    val helper = StructHelper.StructExtension(value)
                    val pkValue = keys
                      .values
                      .map(k => helper.extractValueFromPath(k))
                      .map {
                        case Right(v) => v.get.toString
                        case Left(e) =>
                          throw new ConnectException(
                            s"Unable to find primary key field values [${keys.mkString(",")}] in record in topic [${record.topic()}], " +
                              s"partition [${record.kafkaPartition()}], offset [${record.kafkaOffset()}], ${e.msg}",
                          )
                      }.mkString(sinkSettings.pkDelimiter)

                    // Use the target (and optionally the prefix) to name the GeoAdd key
                    val optionalPrefix =
                      if (Option(KCQL.kcqlConfig.getTarget).isEmpty) "" else KCQL.kcqlConfig.getTarget.trim
                    val sortedSetName = optionalPrefix + pkValue

                    val score = helper.extractValueFromPath(scoreField) match {
                      case Right(Some(v: java.lang.Long)) => v.toDouble
                      case Right(other) =>
                        throw new ConnectException(
                          s"Unable to find score field, or score field in unexpected format, $other, [$scoreField] in record in topic [${record.topic()}], ",
                        )
                      case Left(e) =>
                        throw new ConnectException(
                          s"Unable to find score field [$scoreField] in record in topic [${record.topic()}], " +
                            s"partition [${record.kafkaPartition()}], offset [${record.kafkaOffset()}], ${e.msg}",
                        )
                    }

                    val response = jedis.zadd(sortedSetName, score, payload.toString)

                    if (response == 1) {
                      val ttl = KCQL.kcqlConfig.getTTL
                      if (ttl > 0) {
                        jedis.expire(sortedSetName, ttl)
                      }
                    }

                    response
                  case Failure(_) =>
                    throw new ConnectException(
                      s"Failed to constructed new record with primary key fields [${fields.mkString(",")}] and score fields [${scoreFields.mkString(",")}]",
                    )
                }
              }
            }
          }
          handleTry(t)
        }
        logger.debug(s"Wrote [${sinkRecords.size}] rows for topic [$topic]")
    }

  override def close(): Unit = jedis.close()
}
