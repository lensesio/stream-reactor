package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisKCQLSetting, RedisSinkSettings}
import com.datamountaineer.streamreactor.connect.rowkeys.StringStructFieldsStringKeyBuilder
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
class RedisMultipleSortedSets(sinkSettings: RedisSinkSettings) extends RedisWriter {

  apply(sinkSettings)

  val configs = sinkSettings.allKCQLSettings.map(_.kcqlConfig)
  configs.foreach { c =>
    assert(c.getSource.trim.length > 0, "You need to supply a valid source kafka topic to fetch records from. Review your KCQL syntax")
    assert(c.getPrimaryKeys.length == 1, "The Redis MultipleSortedSets mode requires strictly 1 PK (Primary Key) to be defined")
    assert(c.getStoredAs.equalsIgnoreCase("SortedSet"), "The Redis MultipleSortedSet mode requires the KCQL syntax: STOREAS SortedSet")
  }

  // Write a sequence of SinkRecords to Redis
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty)
      logger.debug("No records received on 'PK SS' Redis writer")
    else {
      logger.debug(s"'PK SS' Redis writer received ${records.size} records")
      insert(records.groupBy(_.topic))
    }
  }

  // Insert a batch of sink records
  def insert(records: Map[String, Seq[SinkRecord]]): Unit = {
    records.foreach({
      case (topic, sinkRecords: Seq[SinkRecord]) => {
        val topicSettings: Set[RedisKCQLSetting] = sinkSettings.allKCQLSettings.filter(_.kcqlConfig.getSource == topic)
        if (topicSettings.isEmpty)
          logger.warn(s"Received a batch for topic $topic - but no KCQL supports it")
        //pass try to error handler and try
        val t = Try(
          {
            sinkRecords.foreach { record =>
              topicSettings.map { KCQL =>
                // Get a SinkRecord
                val recordToSink = convert(record, fields = KCQL.fieldsAndAliases, ignoreFields = KCQL.ignoredFields)
                // Use the target (and optionally the prefix) to name the SortedSet
                val optionalPrefix = KCQL.kcqlConfig.getTarget
                val pkValue = StringStructFieldsStringKeyBuilder(Seq(KCQL.kcqlConfig.getPrimaryKeys.next)).build(recordToSink)
                val sortedSetName = optionalPrefix + pkValue
                val payload = convertValueToJson(recordToSink)
                // How to 'score' each message
                val ssParams = KCQL.kcqlConfig.getStoredAsParameters
                val scoreField = if (ssParams.keys.contains("score"))
                  ssParams.get("score")
                else {
                  logger.info("You have not defined how to 'score' each message. We'll try to fall back to 'timestamp' field")
                  "timestamp"
                }

                val score = StringStructFieldsStringKeyBuilder(Seq(scoreField)).build(recordToSink).toDouble
                logger.debug(s"ZADD $sortedSetName    score = $score     payload = ${payload.toString}")
                val response = jedis.zadd(sortedSetName, score, payload.toString)
                if (response == 1)
                  logger.debug("New element added")
                else if (response == 0)
                  logger.debug("The element was already a member of the sorted set and the score was updated")
                response
              }
            }
          })
        handleTry(t)
      }
        logger.debug(s"Wrote ${sinkRecords.size} rows for topic $topic")
    })
  }

}
