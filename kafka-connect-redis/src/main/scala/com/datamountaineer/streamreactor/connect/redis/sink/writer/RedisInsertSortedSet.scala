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
import com.datamountaineer.streamreactor.common.rowkeys.StringStructFieldsStringKeyBuilder
import com.datamountaineer.streamreactor.common.schemas.SinkRecordConverterHelper.SinkRecordExtension
import com.datamountaineer.streamreactor.connect.json.SimpleJsonConverter
import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisKCQLSetting
import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkSettings
import org.apache.kafka.connect.sink.SinkRecord

import scala.jdk.CollectionConverters.ListHasAsScala
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
    assert(c.getTarget.nonEmpty, "Add to your KCQL syntax : INSERT INTO REDIS_KEY_NAME ")
    assert(
      c.getSource.trim.nonEmpty,
      "You need to define one (1) topic to source data. Add to your KCQL syntax: SELECT * FROM topicName",
    )
    val allFields = if (c.getIgnoredFields.isEmpty) false else true
    assert(
      c.getFields.asScala.nonEmpty || allFields,
      "You need to SELECT at least one field from the topic to be stored in the Redis (sorted) set. Please review the KCQL syntax of connector",
    )
    assert(
      c.getPrimaryKeys.isEmpty,
      s"They keyword PK (Primary Key) is not supported in Redis INSERT_SS mode. Please review the KCQL syntax of connector",
    )
    assert(c.getStoredAs.equalsIgnoreCase("SortedSet"), "This mode requires the KCQL syntax: STOREAS SortedSet")
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
      logger.debug("No records received on 'INSERT SortedSet' Redis writer")
    else {
      logger.debug(s"'INSERT SS' Redis writer received [${records.size}] records")
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
            //pass try to error handler and try
            val t = Try {
              sinkRecords.foreach {
                record =>
                  val struct = record.newFilteredRecordAsStruct(projections)
                  topicSettings.map {
                    KCQL =>
                      // Use the target to name the SortedSet
                      val sortedSetName = KCQL.kcqlConfig.getTarget
                      val payload       = simpleJsonConverter.fromConnectData(struct.schema(), struct)
                      val newRecord = record.newRecord(record.topic(),
                                                       record.kafkaPartition(),
                                                       null,
                                                       null,
                                                       struct.schema(),
                                                       struct,
                                                       record.timestamp(),
                      )
                      val scoreField = getScoreField(KCQL.kcqlConfig)
                      val score      = StringStructFieldsStringKeyBuilder(Seq(scoreField)).build(newRecord).toDouble
                      val response   = jedis.zadd(sortedSetName, score, payload.toString)

                      if (response == 1) {
                        val ttl = KCQL.kcqlConfig.getTTL
                        if (ttl > 0) {
                          jedis.expire(sortedSetName, ttl)
                        }
                      }
                  }
              }
            }
            handleTry(t)
          }
          logger.debug(s"Wrote [${sinkRecords.size}] rows for topic [$topic]")
      },
    )

}
