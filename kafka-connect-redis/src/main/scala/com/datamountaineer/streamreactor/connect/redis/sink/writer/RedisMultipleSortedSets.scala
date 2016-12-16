package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisKCQLSetting, RedisSinkSettings}
import org.apache.kafka.connect.sink.SinkRecord
import scala.collection.JavaConversions._
import scala.util.Try

/**
  * The PK (Primary Key) Redis `writer` stores in 1 Sorted Set / per field value of the PK
  *
  * KCQL syntax requires 1 Primary Key to be defined (plus) STOREAS SS
  *
  *   .. PK .. STOREAS SS
  */
class RedisMultipleSortedSets(sinkSettings: RedisSinkSettings) extends RedisWriter {

  apply(sinkSettings)

  val configs = sinkSettings.allKCQLSettings.map(_.kcqlConfig)
  configs.foreach { c =>
    assert(c.getSource.trim.length > 0, "The source topic seems to be invalid " + c.getSource.trim)
    assert(c.getPrimaryKeys.length == 1, "The Redis PK SS mode requires strictly 1 PK (Primary Key) to be defined")
    assert(c.getStoredAs == "SS", "The Redis PK SS mode requires STOREAS SS")
  }

  // Write a sequence of SinkRecords to Redis
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty)
      logger.debug("No records received on 'PK SS' Redis writer")
    else {
      logger.debug(s"'PK SS' Redis writer received ${records.size} records")
      val grouped = records.groupBy(_.topic())
      insert(grouped)
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
                val keyBuilder = KCQL.builder
                val extracted = convert(record, fields = KCQL.fieldsAndAliases, ignoreFields = KCQL.ignoredFields)
                val key = keyBuilder.build(extracted)
                val payload = convertValueToJson(extracted).toString
                jedis.set(key, payload)
              }
            }
          })
        handleTry(t)
      }
        logger.debug(s"Wrote ${sinkRecords.size} rows for topic $topic")
    })
  }

}
