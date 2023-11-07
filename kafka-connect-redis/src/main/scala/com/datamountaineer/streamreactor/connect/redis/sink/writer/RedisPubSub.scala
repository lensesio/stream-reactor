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
import com.datamountaineer.streamreactor.common.rowkeys.StringStructFieldsStringKeyBuilder
import com.datamountaineer.streamreactor.common.schemas.SinkRecordConverterHelper.SinkRecordExtension
import com.datamountaineer.streamreactor.common.sink.DbWriter
import com.datamountaineer.streamreactor.connect.json.SimpleJsonConverter
import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisKCQLSetting
import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkSettings
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord
import redis.clients.jedis.Jedis

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Try

/**
  * A generic Redis `writer` that can store data into Redis PubSub / KCQL
  *
  * Requires KCQL syntax:   INSERT .. SELECT .. STOREAS PubSub
  *
  * If a field <channel> exists it will automatically be used as the Redis PubSub topic.
  * Otherwise you would need to explicitly define how the values will be scored.
  *
  * Examples:
  *
  * SELECT * from cpuTopic STOREAS PubSub
  * SELECT * from cpuTopic STOREAS PubSub (channel=channel)
  */
class RedisPubSub(sinkSettings: RedisSinkSettings, jedis: Jedis)
    extends DbWriter
    with StrictLogging
    with ErrorHandler
    with PubSubSupport {
  initialize(sinkSettings.taskRetries, sinkSettings.errorPolicy)
  val configs: Set[Kcql] = sinkSettings.kcqlSettings.map(_.kcqlConfig)
  configs.foreach { c =>
    assert(
      c.getSource.trim.nonEmpty,
      "You need to define one (1) topic to source data. Add to your KCQL syntax: SELECT * FROM topicName",
    )
    val allFields = if (c.getIgnoredFields.isEmpty) false else true
    assert(
      c.getFields.asScala.nonEmpty || allFields,
      "You need to SELECT at least one field from the topic to be published to the redis channel. Please review the KCQL syntax of the connector",
    )
    assert(
      c.getPrimaryKeys.isEmpty,
      "They keyword PK (Primary Key) is not supported in Redis PUBSUB mode. Please review the KCQL syntax of connector",
    )
    assert(c.getStoredAs.equalsIgnoreCase("PubSub"), "This mode requires the KCQL syntax: STOREAS PubSub")
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
      logger.debug("No records received on 'PUBSUB' Redis writer")
    else {
      logger.debug(s"'PUBSUB' Redis writer received [${records.size}] records")
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
              sinkRecords.foreach { record =>
                val struct = record.newFilteredRecordAsStruct(projections)
                topicSettings.map { KCQL =>
                  val payload      = simpleJsonConverter.fromConnectData(struct.schema(), struct)
                  val channelField = getChannelField(KCQL.kcqlConfig)
                  val channel      = StringStructFieldsStringKeyBuilder(Seq(channelField)).build(record)
                  val response     = jedis.publish(channel, payload.toString)
                  response
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
