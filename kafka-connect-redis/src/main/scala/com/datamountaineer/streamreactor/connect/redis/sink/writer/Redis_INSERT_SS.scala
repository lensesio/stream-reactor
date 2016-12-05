package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisKCQLSetting, RedisSinkSettings}
import com.datamountaineer.streamreactor.connect.config.Field
import org.apache.kafka.connect.sink.SinkRecord
import scala.collection.JavaConversions._
import scala.util.Try
import java.util.Date

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
class Redis_INSERT_SS(sinkSettings: RedisSinkSettings) extends RedisWriter {

  val configs = sinkSettings.allKCQLSettings.map(_.kcqlConfig)
  var index = 1
  configs.foreach { c =>
    assert(c.getTarget.length == 1, "You need to define a target i.e. INSERT INTO x - and 'x' should be a valid redis key")
    assert(c.getSource.trim.length == 1, "You need to define one (1) topic to source data from (" + c.getSource.trim + ")")
    assert(c.getFieldAlias.nonEmpty && !c.isIncludeAllFields, "You need to SELECT at least one field from the topic to be stored in the Redis (sorted) set. Please review the [$index] KCQL syntax of connector")
    assert(c.getPrimaryKeys.isEmpty, s"They keyword PK (Primary Key) is not supported in Redis INSERT_SS mode. Please review the [$index] KCQL syntax of connector")
    assert(c.getStoredAs == "SS", "This mode requires the KCQL syntax: STOREAS SS ")
    index += 1
  }

  // Write a sequence of SinkRecords to Redis
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty)
      logger.debug("No records received on 'INSERT SS' Redis writer")
    else {
      logger.debug(s"'INSERT SS' Redis writer received ${records.size} records")
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
                // Get a SinkRecord
                val recordToSink = convert(record, fields = KCQL.fieldsAndAliases, ignoreFields = KCQL.ignoredFields)
                // We write into SS named as the target
                val sortedSetName = KCQL.kcqlConfig.getTarget
                val now = new Date().getTime.toDouble
                val payload = convertValueToJson(recordToSink).toString

                //?val keyBuilder = KCQL.builder
                // If sink record contains a time-stamp use it
                if (recordToSink.valueSchema.fields.contains { field: Field => field.name.equalsIgnoreCase("timestamp") }) {
                  // TODO: extract automatically the timestamp to use as score
                }
                jedis.zadd(sortedSetName, now, payload)
              }
            }
          })
        handleTry(t)
      }
        logger.debug(s"Wrote ${sinkRecords.size} rows for topic $topic")
    })
  }

}
