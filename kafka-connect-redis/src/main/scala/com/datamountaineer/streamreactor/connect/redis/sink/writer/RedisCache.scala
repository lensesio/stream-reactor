package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisKCQLSetting, RedisSinkSettings}
import com.datamountaineer.streamreactor.connect.rowkeys.StringStructFieldsStringKeyBuilder
import org.apache.kafka.connect.sink.SinkRecord
import scala.collection.JavaConversions._
import scala.util.Try

/**
  * SELECT price from yahoo-fx PK symbol
  * INSERT INTO FX- SELECT price from yahoo-fx PK symbol
  * SELECT price from yahoo-fx PK symbol WITHEXTRACT
  */
class RedisCache(sinkSettings: RedisSinkSettings) extends RedisWriter {

  val configs = sinkSettings.allKCQLSettings.map(_.kcqlConfig)
  configs.foreach { c =>
    assert(c.getSource.trim.length > 0, "The source topic seems to be invalid " + c.getSource.trim)
    assert(c.getPrimaryKeys.length == 1, "The Key writer requires ONLY 1 PK (Primary Key) to be defined")
    assert(c.getStoredAs == null, "The source topic seems to be invalid " + c.getSource.trim)
  }

  //Write a sequence of SinkRecords to Redis.
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("No records received on 'Cache' Redis writer")
    } else {
      logger.debug(s"Received ${records.size} record for 'Cache' Redis writer")
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
                // We can prefix the name of the <KEY> using the target
                val optionalPrefix = KCQL.kcqlConfig.getTarget.trim
                // Use first primary key's value and (optional) prefix
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
