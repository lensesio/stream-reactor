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
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * A generic Redis `writer` that can store data into Redis streams / KCQL
  *
  * Requires KCQL syntax:   INSERT .. SELECT .. STOREAS stream
  *
  * Examples:
  *
  * INSERT INTO stream1 SELECT * from cpuTopic STOREAS stream
  */
class RedisStreams(sinkSettings: RedisSinkSettings) extends RedisWriter with PubSubSupport {

  val configs: Set[Kcql] = sinkSettings.kcqlSettings.map(_.kcqlConfig)
  configs.foreach { c =>
//    assert(c.getTarget.length > 0, "Add to your KCQL syntax : INSERT INTO REDIS_KEY_NAME ")
    assert(c.getSource.trim.length > 0, "You need to define one (1) topic to source data. Add to your KCQL syntax: SELECT * FROM topicName")
    val allFields = if (c.getIgnoredFields.isEmpty) false else true
    assert(c.getStoredAs.equalsIgnoreCase("Stream"), "This mode requires the KCQL syntax: STOREAS Stream")
  }

  // Write a sequence of SinkRecords to Redis
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty)
      logger.debug("No records received on 'STREAM' Redis writer")
    else {
      logger.debug(s"'STREAM' Redis writer received ${records.size} records")
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
              val jsonPayload = convertValueToJson(recordToSink)
              val payload = Try(new ObjectMapper().convertValue(jsonPayload, classOf[java.util.HashMap[String, Any]])) match {
                case Success(value) =>
                  value.asScala.toMap.map{
                    case(k, v) =>
                      (k, v.toString)
                  }
                case Failure(exception) =>
                  throw new ConnectException(s"Failed to convert payload to key value pairs", exception)
              }
              jedis.xadd(KCQL.kcqlConfig.getTarget, null, payload.asJava)
            }
          }
        }
        handleTry(t)
      }
      logger.debug(s"Published ${sinkRecords.size} messages for topic $topic")
    })
  }
}
