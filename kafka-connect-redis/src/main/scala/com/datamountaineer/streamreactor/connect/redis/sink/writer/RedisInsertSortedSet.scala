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
  * A generic Redis `writer` that can store data into 1 Sorted Set / KCQL
  *
  * Requires KCQL syntax:   INSERT .. SELECT .. STOREAS SortedSet
  *
  * If a field <timestamp> exists it will automatically be used to `score` each value inside the (sorted) set
  * Otherwise you would need to explicitly define how the values will be scored
  *
  * Examples:
  *
  * INSERT INTO cpu_stats SELECT * from cpuTopic STOREAS SortedSet
  * INSERT INTO cpu_stats_SS SELECT * from cpuTopic STOREAS SortedSet (score=ts)
  */
class RedisInsertSortedSet(sinkSettings: RedisSinkSettings) extends RedisWriter with SortedSetSupport {

  val configs: Set[Kcql] = sinkSettings.kcqlSettings.map(_.kcqlConfig)
  configs.foreach { c =>
    assert(c.getTarget.length > 0, "Add to your KCQL syntax : INSERT INTO REDIS_KEY_NAME ")
    assert(c.getSource.trim.length > 0, "You need to define one (1) topic to source data. Add to your KCQL syntax: SELECT * FROM topicName")
    val allFields = if (c.getIgnoredFields.isEmpty) false else true
    assert(c.getFields.asScala.nonEmpty || allFields, "You need to SELECT at least one field from the topic to be stored in the Redis (sorted) set. Please review the KCQL syntax of connector")
    assert(c.getPrimaryKeys.isEmpty, s"They keyword PK (Primary Key) is not supported in Redis INSERT_SS mode. Please review the KCQL syntax of connector")
    assert(c.getStoredAs.equalsIgnoreCase("SortedSet"), "This mode requires the KCQL syntax: STOREAS SortedSet")
  }

  // Write a sequence of SinkRecords to Redis
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty)
      logger.debug("No records received on 'INSERT SortedSet' Redis writer")
    else {
      logger.debug(s"'INSERT SS' Redis writer received ${records.size} records")
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
        //pass try to error handler and try
        val t = Try {
          sinkRecords.foreach { record =>
            topicSettings.map { KCQL =>
              // Get a SinkRecord
              val recordToSink = convert(record, fields = KCQL.fieldsAndAliases, ignoreFields = KCQL.ignoredFields)
              // Use the target to name the SortedSet
              val sortedSetName = KCQL.kcqlConfig.getTarget
              val payload = convertValueToJson(recordToSink)

              val scoreField = getScoreField(KCQL.kcqlConfig)
              val score = StringStructFieldsStringKeyBuilder(Seq(scoreField)).build(record).toDouble

              logger.debug(s"ZADD $sortedSetName    score = $score     payload = ${payload.toString}")
              val response = jedis.zadd(sortedSetName, score, payload.toString)

              if (response == 1) {
                logger.debug("New element added")
                val ttl = KCQL.kcqlConfig.getTTL
                if (ttl > 0) {
                  jedis.expire(sortedSetName, ttl.toInt)
                }
              } else if (response == 0)
                logger.debug("The element was already a member of the sorted set and the score was updated")
              response
            }
          }
        }
        handleTry(t)
      }
        logger.debug(s"Wrote ${sinkRecords.size} rows for topic $topic")
    })
  }

}
