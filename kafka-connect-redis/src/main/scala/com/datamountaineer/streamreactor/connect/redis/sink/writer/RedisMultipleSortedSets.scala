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
import com.datamountaineer.streamreactor.common.config.base.settings.Projections
import com.datamountaineer.streamreactor.common.rowkeys.StringStructFieldsStringKeyBuilder
import com.datamountaineer.streamreactor.common.schemas.SinkRecordConverterHelper.SinkRecordExtension
import com.datamountaineer.streamreactor.common.schemas.StructFieldsExtractor
import com.datamountaineer.streamreactor.connect.json.SimpleJsonConverter
import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisKCQLSetting, RedisSinkSettings}
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * The PK (Primary Key) Redis `writer` stores in 1 Sorted Set / per field value of the PK
  *
  * KCQL syntax requires 1 Primary Key to be defined (plus) STOREAS SortedSet
  *
  * .. PK .. STOREAS SortedSet
  */
class RedisMultipleSortedSets(sinkSettings: RedisSinkSettings) extends RedisWriter with SortedSetSupport {

  private lazy val simpleJsonConverter = new SimpleJsonConverter()

  val configs: Set[Kcql] = sinkSettings.kcqlSettings.map(_.kcqlConfig)
  configs.foreach { c =>
    assert(c.getSource.trim.nonEmpty, "You need to supply a valid source kafka topic to fetch records from. Review your KCQL syntax")
    assert(c.getPrimaryKeys.asScala.nonEmpty, "The Redis MultipleSortedSets mode requires at least 1 PK (Primary Key) to be defined")
    assert(c.getStoredAs.equalsIgnoreCase("SortedSet"), "The Redis MultipleSortedSets mode requires the KCQL syntax: STOREAS SortedSet")
  }
  private val projections = Projections(kcqls = configs, props = Map.empty, errorPolicy = sinkSettings.errorPolicy, errorRetries = sinkSettings.taskRetries, defaultBatchSize = 100)

  // Write a sequence of SinkRecords to Redis
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty)
      logger.debug("No records received on MultipleSortedSets SortedSet Redis writer")
    else {
      logger.debug(s"'MultipleSortedSets' Redis writer received [${records.size}] records")
      insert(records.groupBy(_.topic))
    }
  }

  // Insert a batch of sink records
  def insert(records: Map[String, Seq[SinkRecord]]): Unit = {
    records.foreach {
      case (topic, sinkRecords: Seq[SinkRecord]) => {
        val topicSettings: Set[RedisKCQLSetting] = sinkSettings.kcqlSettings.filter(_.kcqlConfig.getSource == topic)
        if (topicSettings.isEmpty)
          logger.warn(s"No KCQL statement set for [$topic]")
        //pass try to error handler and try
        val t = Try {
          sinkRecords.foreach { record =>

            topicSettings.map { KCQL =>

              // Build a Struct field extractor to get the value from the PK field
              val keys = KCQL.kcqlConfig.getPrimaryKeys.asScala.map(pk => (pk.getName, pk.getName)).toMap
              val scoreField = getScoreField(KCQL.kcqlConfig)

              val pkStruct =
                Try(record.extract(record.value(),
                  record.valueSchema(),
                  KCQL.kcqlConfig.getFields.asScala.map(f => f.getName -> f.getAlias).toMap ++ keys ++ Map(scoreField -> scoreField),
                  Set.empty)) match {
                  case Success(value) => Some(value)
                  case Failure(f) =>
                    logger.warn(s"Failed to constructed new record with fields [${keys.mkString(",")}]. Skipping record")
                    None
                }

              pkStruct match {
                case Some(value) =>
                  val extractor = StructFieldsExtractor(includeAllFields = false, keys)
                  val pkValue = extractor.get(value).toMap.values.mkString(":")

                  // Use the target (and optionally the prefix) to name the GeoAdd key
                  val optionalPrefix = if (Option(KCQL.kcqlConfig.getTarget).isEmpty) "" else KCQL.kcqlConfig.getTarget.trim
                  val sortedSetName = optionalPrefix + pkValue

                  // Include the score into the payload
                  val struct = record.extract(record.value(),
                    record.valueSchema(),
                    KCQL.kcqlConfig.getFields.asScala.map(f => f.getName -> f.getAlias).toMap ++ Map(scoreField -> scoreField),
                    Set.empty)
                  val payload = simpleJsonConverter.fromConnectData(struct.schema(), struct)

                  val newRecord = record.newRecord(record.topic(), record.kafkaPartition(), null, null, value.schema(), value, record.timestamp())
                  val score = StringStructFieldsStringKeyBuilder(Seq(scoreField)).build(newRecord).toDouble

                  logger.debug(s"ZADD [$sortedSetName] score [$score] payload [${payload.toString}]")
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

                case _ =>
                  logger.warn(s"Failed to extract primary keys from record in topic [${record.topic()}], partition [${record.kafkaPartition()}], offset [${record.kafkaOffset()}]")
              }
            }
          }
        }
        handleTry(t)
      }
        logger.debug(s"Wrote [${sinkRecords.size}] rows for topic [$topic]")
    }
  }

}
