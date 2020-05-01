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

package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisKCQLSetting, RedisSinkSettings}
import com.datamountaineer.streamreactor.connect.rowkeys.StringStructFieldsStringKeyBuilder
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConverters._
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
class RedisPubSub(sinkSettings: RedisSinkSettings) extends RedisWriter with PubSubSupport {

  val configs: Set[Kcql] = sinkSettings.kcqlSettings.map(_.kcqlConfig)
  configs.foreach { c =>
//    assert(c.getTarget.length > 0, "Add to your KCQL syntax : INSERT INTO REDIS_KEY_NAME ")
    assert(c.getSource.trim.length > 0, "You need to define one (1) topic to source data. Add to your KCQL syntax: SELECT * FROM topicName")
    val allFields = if (c.getIgnoredFields.isEmpty) false else true
    assert(c.getFields.asScala.nonEmpty || allFields, "You need to SELECT at least one field from the topic to be published to the redis channel. Please review the KCQL syntax of the connector")
    assert(c.getPrimaryKeys.isEmpty, "They keyword PK (Primary Key) is not supported in Redis PUBSUB mode. Please review the KCQL syntax of connector")
    assert(c.getStoredAs.equalsIgnoreCase("PubSub"), "This mode requires the KCQL syntax: STOREAS PubSub")
  }

  // Write a sequence of SinkRecords to Redis
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty)
      logger.debug("No records received on 'PUBSUB' Redis writer")
    else {
      logger.debug(s"'PUBSUB' Redis writer received ${records.size} records")
      insert(records.groupBy(_.topic))
    }
  }

  // Insert a batch of sink records
  def insert(records: Map[String, Seq[SinkRecord]]): Unit = {
    records.foreach({
      case (topic, sinkRecords: Seq[SinkRecord]) => {
        val topicSettings: Set[RedisKCQLSetting] = sinkSettings.kcqlSettings.filter(_.kcqlConfig.getSource == topic)
        if (topicSettings.isEmpty)
          logger.warn(s"Received a batch for topic $topic - but no KCQL supports it")
        val t = Try {
          sinkRecords.foreach { record =>
            topicSettings.map { KCQL =>
              // Get a SinkRecord
              val recordToSink = convert(record, fields = KCQL.fieldsAndAliases, ignoreFields = KCQL.ignoredFields)
              // Use the target to name the SortedSet
              val payload = convertValueToJson(recordToSink)

              val channelField = getChannelField(KCQL.kcqlConfig)
              val channel = StringStructFieldsStringKeyBuilder(Seq(channelField)).build(record)

              logger.debug(s"PUBLISH $channel  channel = $channel  payload = ${payload.toString}")
              val response = jedis.publish(channel, payload.toString)

              logger.debug(s"Published a new message to $response clients.")
              response
            }
          }
        }
        handleTry(t)
      }
      logger.debug(s"Published ${sinkRecords.size} messages for topic $topic")
    })
  }

}
