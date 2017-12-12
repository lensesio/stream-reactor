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

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
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
class RedisCache(sinkSettings: RedisSinkSettings) extends RedisWriter {

  apply(sinkSettings)

  val configs: Set[Kcql] = sinkSettings.kcqlSettings.map(_.kcqlConfig)
  configs.foreach { c =>
    assert(c.getSource.trim.length > 0, "You need to supply a valid source kafka topic to fetch records from. Review your KCQL syntax")
    assert(c.getPrimaryKeys.nonEmpty, "The Redis CACHE mode requires at least 1 PK (Primary Key) to be defined")
    assert(c.getStoredAs == null, "The Redis CACHE mode does not support STOREAS")
  }

  // Write a sequence of SinkRecords to Redis
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("No records received on 'Cache' Redis writer")
    } else {
      logger.debug(s"'Cache' Redis writer received ${records.size} records")
      insert(records.groupBy(_.topic))
    }
  }

  // Insert a batch of sink records
  def insert(records: Map[String, Seq[SinkRecord]]): Unit = {
    records.foreach {
      case (topic, sinkRecords) => {
        val topicSettings: Set[RedisKCQLSetting] = sinkSettings.kcqlSettings.filter(_.kcqlConfig.getSource == topic)
        if (topicSettings.isEmpty)
          logger.warn(s"Received a batch for topic $topic - but no KCQL supports it")
        //pass try to error handler and try
        val t = Try(
          {
            sinkRecords.foreach { record =>
              topicSettings.map { KCQL =>
                // We can prefix the name of the <KEY> using the target
                val optionalPrefix = if (Option(KCQL.kcqlConfig.getTarget).isEmpty) "" else KCQL.kcqlConfig.getTarget.trim
                // Use first primary key's value and (optional) prefix
                val keyBuilder = StringStructFieldsStringKeyBuilder(KCQL.kcqlConfig.getPrimaryKeys.map(_.getName))
                val extracted = convert(record, fields = KCQL.fieldsAndAliases, ignoreFields = KCQL.ignoredFields)
                val key = optionalPrefix + keyBuilder.build(record)
                val payload = convertValueToJson(extracted).toString
                jedis.set(key, payload)
              }
            }
          })
        handleTry(t)
      }
        logger.debug(s"Wrote ${sinkRecords.size} rows for topic $topic")
    }
  }

}
