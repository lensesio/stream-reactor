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
import com.datamountaineer.streamreactor.connect.schemas.StructFieldsExtractor
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * The PK (Primary Key) Redis `writer` stores in 1 Sorted Set / per field value of the PK
  *
  * KCQL syntax requires 1 Primary Key to be defined (plus) STOREAS SortedSet
  *
  * .. PK .. STOREAS SortedSet
  */
class RedisMultipleSortedSets(sinkSettings: RedisSinkSettings) extends RedisWriter with SortedSetSupport {

  apply(sinkSettings)

  val configs: Set[Kcql] = sinkSettings.kcqlSettings.map(_.kcqlConfig)
  configs.foreach { c =>
    assert(c.getSource.trim.length > 0, "You need to supply a valid source kafka topic to fetch records from. Review your KCQL syntax")
    assert(c.getPrimaryKeys.length >= 1, "The Redis MultipleSortedSets mode requires at least 1 PK (Primary Key) to be defined")
    assert(c.getStoredAs.equalsIgnoreCase("SortedSet"), "The Redis MultipleSortedSets mode requires the KCQL syntax: STOREAS SortedSet")
  }

  // Write a sequence of SinkRecords to Redis
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty)
      logger.debug("No records received on 'MultipleSortedSets SortedSet' Redis writer")
    else {
      logger.debug(s"'MultipleSortedSets' Redis writer received ${records.size} records")
      insert(records.groupBy(_.topic))
    }
  }

  // Insert a batch of sink records
  def insert(records: Map[String, Seq[SinkRecord]]): Unit = {
    records.foreach {
      case (topic, sinkRecords: Seq[SinkRecord]) => {
        val topicSettings: Set[RedisKCQLSetting] = sinkSettings.kcqlSettings.filter(_.kcqlConfig.getSource == topic)
        if (topicSettings.isEmpty)
          logger.warn(s"Received a batch for topic $topic - but no KCQL supports it")
        //pass try to error handler and try
        val t = Try {
          sinkRecords.foreach { record =>
            topicSettings.map { KCQL =>

              // Build a Struct field extractor to get the value from the PK field
              //val pkField = KCQL.kcqlConfig.getPrimaryKeys.toList.head
              val extractor = StructFieldsExtractor(includeAllFields = false, KCQL.kcqlConfig.getPrimaryKeys.map(f=>f.getName-> f.getName).toMap)
              val fieldsAndValues = extractor.get(record.value.asInstanceOf[Struct]).toMap
              val pkValue = KCQL.kcqlConfig.getPrimaryKeys.map(pk=>fieldsAndValues(pk.getName).toString).mkString(":")

              // Use the target (and optionally the prefix) to name the SortedSet
              val optionalPrefix = if (Option(KCQL.kcqlConfig.getTarget).isEmpty) "" else KCQL.kcqlConfig.getTarget.trim
              val sortedSetName = optionalPrefix + pkValue

              // Include the score into the payload
              val scoreField = getScoreField(KCQL.kcqlConfig)
              val recordToSink = convert(record, fields = KCQL.fieldsAndAliases + (scoreField -> scoreField), ignoreFields = KCQL.ignoredFields)
              val payload = convertValueToJson(recordToSink)

              val score = StringStructFieldsStringKeyBuilder(Seq(scoreField)).build(record).toDouble

              logger.debug(s"ZADD $sortedSetName    score = $score     payload = ${payload.toString}")
              val response = jedis.zadd(sortedSetName, score, payload.toString)

              if (response == 1)
                logger.debug("New element added")
              else if (response == 0)
                logger.debug("The element was already a member of the sorted set and the score was updated")
              response
            }
          }
        }
        handleTry(t)
      }
        logger.debug(s"Wrote ${sinkRecords.size} rows for topic $topic")
    }
  }

}
