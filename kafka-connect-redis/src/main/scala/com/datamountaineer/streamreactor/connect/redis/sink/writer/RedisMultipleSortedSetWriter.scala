package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisKCQLSetting, RedisSinkSettings}
import org.apache.kafka.connect.sink.SinkRecord
import scala.collection.JavaConversions._
import scala.util.Try

class RedisMultipleSortedSetWriter(sinkSettings: RedisSinkSettings) extends RedisWriter {

  /**
    * Write a sequence of SinkRecords to Redis.
    * Groups the records by topic
    **/
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("No records received on 'Multiple SS' Redis writer")
    } else {
      logger.debug(s"Received ${records.size} records.")
      val grouped = records.groupBy(_.topic())
      insert(grouped)
    }
  }

  /**
    * Insert a batch of sink records
    **/
  def insert(records: Map[String, Seq[SinkRecord]]): Unit = {
    records.foreach({
      case (topic, sinkRecords: Seq[SinkRecord]) => {
        val topicSettings: Set[RedisKCQLSetting] = sinkSettings.allKCQLSettings.filter(_.kcqlConfig.getSource == topic)
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
