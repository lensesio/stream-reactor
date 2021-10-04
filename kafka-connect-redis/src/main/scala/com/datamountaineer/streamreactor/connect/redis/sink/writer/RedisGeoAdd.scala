package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.common.config.base.settings.Projections
import com.datamountaineer.streamreactor.common.schemas.SinkRecordConverterHelper.SinkRecordExtension
import com.datamountaineer.streamreactor.common.schemas.StructFieldsExtractor
import com.datamountaineer.streamreactor.connect.json.SimpleJsonConverter
import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisKCQLSetting, RedisSinkSettings}
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConverters._
import scala.util.control.Exception.allCatch
import scala.util.{Failure, Success, Try}

class RedisGeoAdd(sinkSettings: RedisSinkSettings) extends RedisWriter with GeoAddSupport {

  private lazy val simpleJsonConverter = new SimpleJsonConverter()

  val configs: Set[Kcql] = sinkSettings.kcqlSettings.map(_.kcqlConfig)

  configs.foreach { c =>
    assert(c.getSource.trim.nonEmpty, "You need to supply a valid source kafka topic to fetch records from. Review your KCQL syntax")
    assert(c.getPrimaryKeys.asScala.nonEmpty, "The Redis GeoAdd mode requires at least 1 PK (Primary Key) to be defined")
    assert(c.getStoredAs.equalsIgnoreCase("GeoAdd"), "The Redis GeoAdd mode requires the KCQL syntax: STOREAS GeoAdd")
  }
  private val projections = Projections(kcqls = configs, props = Map.empty, errorPolicy = sinkSettings.errorPolicy, errorRetries = sinkSettings.taskRetries, defaultBatchSize = 100)


  // Write a sequence of SinkRecords to Redis
  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty)
      logger.debug("No records received on 'GeoAdd' Redis writer")
    else {
      logger.debug(s"'GeoAdd' Redis writer received [${records.size}] records")
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
            val struct = record.newFilteredRecordAsStruct(projections)
            topicSettings.map { KCQL =>

              val longitudeField = getLongitudeField(KCQL.kcqlConfig)
              val latitudeField = getLatitudeField(KCQL.kcqlConfig)
              val keys = KCQL.kcqlConfig.getPrimaryKeys.asScala.map(pk => (pk.getName, pk.getName)).toMap

              val pkFieldStruct =
                Try(record.extract(record.value(),
                  record.valueSchema(),
                  keys ++ Map(latitudeField -> latitudeField) ++ Map(longitudeField -> longitudeField),
                  Set.empty)) match {
                case Success(value) => Some(value)
                case Failure(f) =>
                  logger.warn(s"Failed to constructed new record with fields [${keys.mkString(",")}, ${longitudeField}, ${latitudeField}]. Skipping record")
                  None
              }

              pkFieldStruct match {
                case Some(value) =>
                  val extractor = StructFieldsExtractor(includeAllFields = false, keys)
                  val pkValue = extractor.get(value).toMap.values.mkString(":")

                  // Use the target (and optionally the prefix) to name the GeoAdd key
                  val optionalPrefix = if (Option(KCQL.kcqlConfig.getTarget).isEmpty) "" else KCQL.kcqlConfig.getTarget.trim
                  val key = optionalPrefix + pkValue
                  val payload = simpleJsonConverter.fromConnectData(struct.schema(), struct)
                  val longitude = value.getString(longitudeField)
                  val latitude = value.getString(latitudeField)

                  if (isDoubleNumber(longitude) && isDoubleNumber(latitude)) {

                    logger.debug(s"GEOADD [$key] longitude [$longitude] latitude [$latitude] payload [${payload.toString}]")
                    val response = jedis.geoadd(key, longitude.toDouble, latitude.toDouble, payload.toString)

                    if (response == 1) {
                      logger.debug("New element added")
                    } else if (response == 0)
                      logger.debug("The element was already a member of the sorted set and the score was updated")
                    response
                  }
                  else {
                    logger.warn(s"GeoAdd record contains invalid longitude [$longitude] and latitude [$latitude] values, " +
                      s"Record with key [${record.key}] is skipped");
                    None
                  }

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


  def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined
}
