package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisKCQLSetting, RedisSinkSettings}
import com.datamountaineer.streamreactor.connect.schemas.StructFieldsExtractor
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.Exception.allCatch

class RedisGeoAdd(sinkSettings: RedisSinkSettings) extends RedisWriter with GeoAddSupport {

  val configs: Set[Kcql] = sinkSettings.kcqlSettings.map(_.kcqlConfig)
  configs.foreach { c =>
    assert(c.getSource.trim.length > 0, "You need to supply a valid source kafka topic to fetch records from. Review your KCQL syntax")
    assert(c.getPrimaryKeys.asScala.length >= 1, "The Redis GeoAdd mode requires at least 1 PK (Primary Key) to be defined")
    assert(c.getStoredAs.equalsIgnoreCase("GeoAdd"), "The Redis GeoAdd mode requires the KCQL syntax: STOREAS GeoAdd")
  }

  // Write a sequence of SinkRecords to Redis
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty)
      logger.debug("No records received on 'GeoAdd' Redis writer")
    else {
      logger.debug(s"'GeoAdd' Redis writer received ${records.size} records")
      insert(records.groupBy(_.topic))
    }
  }

  // Insert a batch of sink records
  def insert(records: Map[String, Seq[SinkRecord]]): Unit = {
    records.foreach {
      case (topic, sinkRecords: Seq[SinkRecord]) => {
        val topicSettings: Set[RedisKCQLSetting] = sinkSettings.kcqlSettings.filter(_.kcqlConfig.getSource == topic)
        if (topicSettings.isEmpty)
          logger.warn(s"Received a batch for topic $topic - but no KCQL supports it")
        //pass try to error handler and try
        val t = Try {
          sinkRecords.foreach { record =>
            topicSettings.map { KCQL =>

              val extractor = StructFieldsExtractor(includeAllFields = false, KCQL.kcqlConfig.getPrimaryKeys.asScala.map(f => f.getName -> f.getName).toMap)
              val fieldsAndValues = extractor.get(record.value.asInstanceOf[Struct]).toMap
              val pkValue = KCQL.kcqlConfig.getPrimaryKeys.asScala.map(pk => fieldsAndValues(pk.getName).toString).mkString(":")

              // Use the target (and optionally the prefix) to name the GeoAdd key
              val optionalPrefix = if (Option(KCQL.kcqlConfig.getTarget).isEmpty) "" else KCQL.kcqlConfig.getTarget.trim
              val key = optionalPrefix + pkValue

              val recordToSink = convert(record, fields = KCQL.fieldsAndAliases, ignoreFields = KCQL.ignoredFields)
              val payload = convertValueToJson(recordToSink)

              val longitudeField = getLongitudeField(KCQL.kcqlConfig)
              val latitudeField = getLatitudeField(KCQL.kcqlConfig)
              val longitude = getFieldValue(record, longitudeField)
              val latitude = getFieldValue(record, latitudeField)

              if (isDoubleNumber(longitude) && isDoubleNumber(latitude)) {

                logger.debug(s"GEOADD $key longitude=$longitude latitude=$latitude payload = ${payload.toString}")
                val response = jedis.geoadd(key, longitude.toDouble, latitude.toDouble, payload.toString)

                if (response == 1) {
                  logger.debug("New element added")
                } else if (response == 0)
                  logger.debug("The element was already a member of the sorted set and the score was updated")
                response
              }
              else {
                logger.warn(s"GeoAdd record contains invalid longitude=$longitude and latitude=$latitude values, " +
                  s"Record with key ${record.key} is skipped");
                None
              }
            }
          }
        }
        handleTry(t)
      }
        logger.debug(s"Wrote ${sinkRecords.size} rows for topic $topic")
    }
  }

  def getFieldValue(record: SinkRecord, fieldName: String): String = {
    val struct = record.value().asInstanceOf[Struct]
    struct.getString(fieldName)
  }

  def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined
}
