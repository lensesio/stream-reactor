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
package io.lenses.streamreactor.connect.redis.sink.writer

import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.config.base.settings.Projections
import io.lenses.streamreactor.common.schemas.SinkRecordConverterHelper.SinkRecordExtension
import io.lenses.streamreactor.connect.json.SimpleJsonConverter
import io.lenses.streamreactor.connect.redis.sink.config.RedisKCQLSetting
import io.lenses.streamreactor.connect.redis.sink.config.RedisSinkSettings
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.common.sink.DbWriter
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.XAddParams

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * A generic Redis `writer` that can store data into Redis streams / KCQL
  *
  * Requires KCQL syntax:   INSERT .. SELECT .. STOREAS stream
  *
  * Examples:
  *
  * INSERT INTO stream1 SELECT * from cpuTopic STOREAS stream
  */
class RedisStreams(sinkSettings: RedisSinkSettings, jedis: Jedis)
    extends DbWriter
    with StrictLogging
    with ErrorHandler
    with PubSubSupport {
  initialize(sinkSettings.taskRetries, sinkSettings.errorPolicy)

  val configs: Set[Kcql] = sinkSettings.kcqlSettings.map(_.kcqlConfig)
  configs.foreach { c =>
//    assert(c.getTarget.length > 0, "Add to your KCQL syntax : INSERT INTO REDIS_KEY_NAME ")
    assert(
      c.getSource.trim.nonEmpty,
      "You need to define one (1) topic to source data. Add to your KCQL syntax: SELECT * FROM topicName",
    )
    assert(c.getStoredAs.equalsIgnoreCase("Stream"), "This mode requires the KCQL syntax: STOREAS Stream")
  }

  private lazy val simpleJsonConverter = new SimpleJsonConverter()
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
      logger.debug("No records received on 'STREAM' Redis writer")
    else {
      logger.debug(s"'STREAM' Redis writer received [${records.size}] records")
      insert(records.groupBy(_.topic))
    }

  // Insert a batch of sink records
  def insert(records: Map[String, Seq[SinkRecord]]): Unit =
    records.foreach(
      {
        case (topic, sinkRecords: Seq[SinkRecord]) => {
            val topicSettings: Set[RedisKCQLSetting] = sinkSettings.kcqlSettings.filter(_.kcqlConfig.getSource == topic)
            if (topicSettings.isEmpty)
              logger.warn(s"No KCQL statement set for [$topic]")
            val t = Try {
              sinkRecords.foreach {
                record =>
                  val struct = record.newFilteredRecordAsStruct(projections)
                  topicSettings.map {
                    KCQL =>
                      val jsonNode = simpleJsonConverter.fromConnectData(struct.schema(), struct)
                      val payload =
                        Try(new ObjectMapper().convertValue(jsonNode, classOf[java.util.HashMap[String, Any]])) match {
                          case Success(value) =>
                            value.asScala.toMap.map {
                              case (k, v) =>
                                (k, v.toString)
                            }
                          case Failure(exception) =>
                            throw new ConnectException(s"Failed to convert payload to key value pairs", exception)
                        }
                      jedis.xadd(KCQL.kcqlConfig.getTarget, new XAddParams, payload.asJava)
                  }
              }
            }
            handleTry(t)
          }
          logger.debug(s"Published [${sinkRecords.size}] messages for topic [$topic]")
      },
    )
  override def close(): Unit = jedis.close()
}
