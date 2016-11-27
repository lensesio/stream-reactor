package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisKCQLSetting, RedisSinkSettings}
import com.datamountaineer.streamreactor.connect.rowkeys.StringStructFieldsStringKeyBuilder
import org.apache.kafka.connect.sink.SinkRecord
import scala.collection.JavaConversions._
import scala.util.Try

class RedisKeyWriter(sinkSettings: RedisSinkSettings) extends RedisWriter {

  val configs = sinkSettings.allKCQLSettings.map(_.kcqlConfig)
  configs.foreach { c =>
    assert(c.getSource.trim.length > 1,  "The source topic seems to be invalid " + c.getSource.trim)
    assert(c.getPrimaryKeys.length == 1, "The Key writer requires ONLY 1 PK (Primary Key) to be defined")
    assert(c.getStoredAs == null, "The source topic seems to be invalid " + c.getSource.trim)
  }

  /**
    * Write a sequence of SinkRecords to Redis.
    * Groups the records by topic
    **/
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("No records received by Redis writer: 'Cache mode'")
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
                val optionalPrefix = KCQL.kcqlConfig.getTarget.trim
                val keyBuilder = StringStructFieldsStringKeyBuilder(Seq(KCQL.kcqlConfig.getPrimaryKeys.next))
                val extracted = convert(record, fields = KCQL.fieldsAndAliases, ignoreFields = KCQL.ignoredFields)
                val key = optionalPrefix + keyBuilder.build(extracted)
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
