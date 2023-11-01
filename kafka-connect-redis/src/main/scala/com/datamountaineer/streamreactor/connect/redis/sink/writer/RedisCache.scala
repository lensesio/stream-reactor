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
package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.common.config.base.settings.Projections
import com.datamountaineer.streamreactor.common.errors.ErrorHandler
import com.datamountaineer.streamreactor.common.schemas.SinkRecordConverterHelper.SinkRecordExtension
import com.datamountaineer.streamreactor.common.schemas.StructHelper
import com.datamountaineer.streamreactor.common.sink.DbWriter
import com.datamountaineer.streamreactor.connect.json.SimpleJsonConverter
import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisKCQLSetting
import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkSettings
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import redis.clients.jedis.Jedis

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * The Redis CACHE mode can store a snapshot a time-series into a Key
  * so that u can always extract the latest (most recent) value from Redis
  *
  * Example KCQL syntax:
  *
  * SELECT price from yahoo-fx PK symbol
  * INSERT INTO FX- SELECT price from yahoo-fx PK symbol
  * SELECT price from yahoo-fx PK symbol WITHEXTRACT
  */
class RedisCache(sinkSettings: RedisSinkSettings, jedis: Jedis) extends DbWriter with StrictLogging with ErrorHandler {
  initialize(sinkSettings.taskRetries, sinkSettings.errorPolicy)
  private lazy val simpleJsonConverter = new SimpleJsonConverter()
  val configs: Set[Kcql] = sinkSettings.kcqlSettings.map(_.kcqlConfig)
  configs.foreach { c =>
    assert(
      c.getSource.trim.nonEmpty,
      "You need to supply a valid source kafka topic to fetch records from. Review your KCQL syntax",
    )
    assert(c.getPrimaryKeys.asScala.nonEmpty, "The Redis CACHE mode requires at least 1 PK (Primary Key) to be defined")
    assert(c.getStoredAs == null, "The Redis CACHE mode does not support STOREAS")
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
    if (records.isEmpty) {
      logger.debug("No records received on 'Cache' Redis writer")
    } else {
      logger.debug(s"'Cache' Redis writer received [${records.size}] records")
      insert(records.groupBy(_.topic))
    }

  // Insert a batch of sink records
  def insert(records: Map[String, Seq[SinkRecord]]): Unit =
    records.foreach {
      case (topic, sinkRecords) => {
          val topicSettings: Set[RedisKCQLSetting] = sinkSettings.kcqlSettings.filter(_.kcqlConfig.getSource == topic)
          if (topicSettings.isEmpty)
            logger.warn(s"No KCQL statement set for [$topic]")
          //pass try to error handler and try
          val t = Try {
            sinkRecords.foreach { record =>
              val struct = record.newFilteredRecordAsStruct(projections)

              topicSettings.map { KCQL =>
                // We can prefix the name of the <KEY> using the target
                val optionalPrefix =
                  if (Option(KCQL.kcqlConfig.getTarget).isEmpty) "" else KCQL.kcqlConfig.getTarget.trim
                //extract will flatten if parent/child detected
                val keys = KCQL.kcqlConfig.getPrimaryKeys.asScala.map(pk =>
                  (pk.toString, pk.toString.replaceAll("\\.", "_")),
                ).toMap

                Try(record.extract(
                  record.value(),
                  record.valueSchema(),
                  keys,
                  Set.empty,
                )) match {
                  case Success(value) =>
                    val helper = StructHelper.StructExtension(value)
                    val pkValue = keys
                      .values
                      .map(helper.extractValueFromPath)
                      .map {
                        case Right(v) => v.get.toString
                        case Left(e) =>
                          throw new ConnectException(
                            s"Unable to find all primary key field values [${keys.mkString(",")}] in record in topic [${record.topic()}], " +
                              s"partition [${record.kafkaPartition()}], offset [${record.kafkaOffset()}], ${e.msg}",
                          )
                      }.mkString(sinkSettings.pkDelimiter)

                    val key = optionalPrefix + pkValue

                    val payload = simpleJsonConverter.fromConnectData(struct.schema(), struct).toString
                    val ttl     = KCQL.kcqlConfig.getTTL
                    if (ttl <= 0) {
                      jedis.set(key, payload)
                    } else {
                      jedis.setex(key, ttl, payload)
                    }

                  case Failure(_) =>
                    throw new ConnectException(
                      s"Failed to constructed new record with primary key fields [${keys.mkString(",")}]",
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
