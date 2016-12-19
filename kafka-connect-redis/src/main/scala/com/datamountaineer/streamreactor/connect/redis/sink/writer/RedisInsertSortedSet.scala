package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisKCQLSetting, RedisSinkSettings}
import com.datamountaineer.streamreactor.connect.rowkeys.StringStructFieldsStringKeyBuilder
import org.apache.kafka.connect.sink.SinkRecord
import scala.collection.JavaConversions._
import scala.util.Try

/**
  * A generic Redis `writer` that can store data into 1 Sorted Set / KCQL
  *
  * Requires KCQL syntax:   INSERT .. SELECT .. STOREAS SS
  *
  * If a field <timestamp> exists it will automatically be used to `score` each value inside the (sorted) set
  * Otherwise you would need to explicitly define how the values will be scored
  *
  * Examples:
  *
  * INSERT INTO cpu_stats SELECT * from cpuTopic STOREAS SS
  * INSERT INTO cpu_stats_SS SELECT * from cpuTopic STOREAS SS (score=ts)
  */
class RedisInsertSortedSet(sinkSettings: RedisSinkSettings) extends RedisWriter {

  apply(sinkSettings)

  val configs = sinkSettings.allKCQLSettings.map(_.kcqlConfig)
  configs.foreach { c =>
    assert(c.getTarget.length > 0, "Add to your KCQL systax : INSERT INTO REDIS_KEY_NAME ")
    assert(c.getSource.trim.length > 0, "You need to define one (1) topic to source data. Add to your KCQL syntax: SELECT * FROM topicName")
    assert(c.getFieldAlias.nonEmpty || c.isIncludeAllFields, "You need to SELECT at least one field from the topic to be stored in the Redis (sorted) set. Please review the [$index] KCQL syntax of connector")
    assert(c.getPrimaryKeys.isEmpty, s"They keyword PK (Primary Key) is not supported in Redis INSERT_SS mode. Please review the KCQL syntax of connector")
    assert(c.getStoredAs == "SS", "This mode requires the KCQL syntax: STOREAS SS")
  }

  // Write a sequence of SinkRecords to Redis
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty)
      logger.debug("No records received on 'INSERT SS' Redis writer")
    else {
      logger.debug(s"'INSERT SS' Redis writer received ${records.size} records")
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
                // We write into SS named as the target
                val sortedSetName = KCQL.kcqlConfig.getTarget
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
