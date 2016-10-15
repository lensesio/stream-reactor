/**
  * Copyright 2016 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.influx.writers

import java.util.concurrent.TimeUnit

import com.datamountaineer.streamreactor.connect.influx.config.InfluxSettings
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import io.confluent.common.config.ConfigException
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.influxdb.InfluxDB.ConsistencyLevel
import org.influxdb.dto.{BatchPoints, Point}

import scala.collection.JavaConversions._
import scala.util.Try


object InfluxBatchPointsBuilderFn extends ConverterUtil {
  def apply(records: Seq[SinkRecord],
            settings: InfluxSettings): BatchPoints = {

    def handleSinkRecord(record: SinkRecord): Option[Point] = {

      Option(record.valueSchema()) match {
        case None =>
          //try to take it as string
          record.value() match {
            case map: java.util.Map[_, _] => buildPointFromMap(record, settings)
            case _ => sys.error("For schemaless record only String and Map types are supported")
          }
        case Some(schema: Schema) =>
          schema.`type`() match {
            case Schema.Type.STRING => buildPointFromJson(record, settings)
            case Schema.Type.STRUCT => buildPointFromStruct(record, settings)
            case other => sys.error(s"$other schema is not supported")
          }
      }
    }
    val batchPoints = BatchPoints
      .database(settings.database)
      .tag("async", "true")
      .retentionPolicy(settings.retentionPolicy)
      .consistency(ConsistencyLevel.ALL)
      .build()

    records.foreach(r => handleSinkRecord(r).map(batchPoints.point))
    batchPoints
  }


  private def buildPointFromMap(record: SinkRecord, settings: InfluxSettings): Option[Point] = {
    require(record.value() != null && record.value().getClass == classOf[java.util.HashMap[_, _]],
      "The SinkRecord payload should be of type java.util.Map[String, Any]")

    val map = record.value().asInstanceOf[java.util.Map[String, Any]]

    val extractor = settings.fieldsExtractorMap(record.topic())
    val timestamp = extractor.timestampField.map { field =>
      map.getOrElse(field,
        throw new ConfigException(s"$field has not been found on the record on topic ${record.topic()} partition:${record.kafkaPartition()} and offset:${record.kafkaOffset()}"))
      match {
        case b: Byte => b.toLong
        case s: Short => s.toLong
        case i: Int => i.toLong
        case l: Long => l
        case _ => throw new ConfigException(s"$field is not a valid field for the timestamp")
      }
    }.getOrElse(System.currentTimeMillis())

    val convertedMap = convertSchemalessJson(record, extractor.fieldsAliasMap, extractor.ignoredFields, false, extractor.includeAllFields)

    fromMapToPoint(convertedMap, timestamp, settings, record)
  }

  private def fromMapToPoint(map: java.util.Map[String, Any],
                             timestamp: Long,
                             settings: InfluxSettings,
                             record: SinkRecord): Option[Point] = {
    if (map.nonEmpty) {
      val builder = Point.measurement(settings.topicToMeasurementMap.getOrElse(record.topic(),
        throw new ConfigException(s"No matching measurement for topic ${record.topic}")))
        .time(timestamp, TimeUnit.MILLISECONDS)

      map.foreach {
        case (field, value: Long) => builder.addField(field, value)
        case (field, value: Int) => builder.addField(field, value)
        case (field, value: BigInt) => builder.addField(field, value)
        case (field, value: Byte) => builder.addField(field, value)
        case (field, value: Short) => builder.addField(field, value)
        case (field, value: Double) => builder.addField(field, value)
        case (field, value: Float) => builder.addField(field, value)
        case (field, value: Boolean) => builder.addField(field, value)
        case (field, value: java.math.BigDecimal) => builder.addField(field, value)
        case (field, value: String) => builder.addField(field, value)
        //we should never reach this since the extractor should not allow it
        case (field, value) => sys.error(s"$value (${Option(value).map(_.getClass.getName).getOrElse("")})is not a valid type for InfluxDb. Allowed types:Boolean, " +
          s"Long, String, Double and Number")
      }
      Some(builder.build())
    }
    else {
      None
    }
  }

  private def buildPointFromJson(record: SinkRecord, settings: InfluxSettings): Option[Point] = {
    require(record.value() != null && record.value().getClass == classOf[String],
      "The SinkRecord payload should be of type String")

    val jsonPayload = record.value().asInstanceOf[String]
    val extractor = settings.fieldsExtractorMap(record.topic())
    val jvalue = convertStringSchemaAndJson(record, extractor.fieldsAliasMap, extractor.ignoredFields, false, extractor.includeAllFields)

    import org.json4s._
    implicit val formats = DefaultFormats
    val map = jvalue.extract[Map[String, Any]]

    val timestamp = extractor.timestampField.map { field =>
      map.getOrElse(field,
        throw new ConfigException(s"$field has not been found on the record on topic ${record.topic()} partition:${record.kafkaPartition()} and offset:${record.kafkaOffset()}"))
      match {
        case b: Byte => b.toLong
        case s: Short => s.toLong
        case i: Int => i.toLong
        case bi: BigInt => bi.toLong
        case l: Long => l
        case s: String => Try(s.toLong).getOrElse(sys.error(s"$field is not a valid field for the timestamp"))
        case _ => sys.error(s"$field is not a valid field for the timestamp")
      }
    }.getOrElse(System.currentTimeMillis())

    fromMapToPoint(map, timestamp, settings, record)
  }


  private def buildPointFromStruct(record: SinkRecord, settings: InfluxSettings): Option[Point] = {
    require(record.value() != null && record.value().getClass == classOf[Struct],
      "The SinkRecord payload should be of type Struct")

    //we want to error if the topic hasn;t been
    val extractor = settings.fieldsExtractorMap(record.topic())
    val recordData = extractor.get(record.value.asInstanceOf[Struct])
    if (recordData.fields.nonEmpty) {
      val pointBuilder = Point.measurement(settings.topicToMeasurementMap.getOrElse(record.topic(),
        throw new ConfigException(s"No matching measurement for topic ${record.topic}")))
        .time(recordData.timestamp, TimeUnit.MILLISECONDS)

      recordData.fields
        .foldLeft(pointBuilder) {
          case (builder, (field, value: Long)) => builder.addField(field, value)
          case (builder, (field, value: Int)) => builder.addField(field, value)
          case (builder, (field, value: Byte)) => builder.addField(field, value)
          case (builder, (field, value: Short)) => builder.addField(field, value)
          case (builder, (field, value: Double)) => builder.addField(field, value)
          case (builder, (field, value: Float)) => builder.addField(field, value)
          case (builder, (field, value: Boolean)) => builder.addField(field, value)
          case (builder, (field, value: java.math.BigDecimal)) => builder.addField(field, value)
          case (builder, (field, value: String)) => builder.addField(field, value)
          //we should never reach this since the extractor should not allow it
          case (builder, (field, value)) => sys.error(s"$value is not a valid type for InfluxDb.Allowed types:Boolean, " +
            s"Long, String, Double and Number")
        }
      Some(pointBuilder.build())
    }
    else {
      None
    }
  }
}
