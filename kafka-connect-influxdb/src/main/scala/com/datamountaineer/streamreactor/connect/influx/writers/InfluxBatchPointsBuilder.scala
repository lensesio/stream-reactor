/*
 * Copyright 2017 Datamountaineer.
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
 */

package com.datamountaineer.streamreactor.connect.influx.writers

import java.util.concurrent.TimeUnit

import com.datamountaineer.kcql.{Field, Kcql, Tag}
import com.datamountaineer.streamreactor.connect.influx.NanoClock
import com.datamountaineer.streamreactor.connect.influx.config.InfluxSettings
import com.landoop.json.sql.JacksonJson
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.influxdb.dto.{BatchPoints, Point}

import scala.collection.JavaConversions._
import scala.concurrent.duration.TimeUnit


/**
  * Builds an InfluxDb point for the incoming SinkRecord.
  * It handles SinkRecord payloads as :
  *   - Kafka Connect Struct
  *   - Java Map
  *   - Json String
  *
  * @param settings - An instance of [[InfluxSettings]]
  */
class InfluxBatchPointsBuilder(settings: InfluxSettings, nanoClock: NanoClock) extends StrictLogging {
  //  private val nanoClock = new NanoClock()

  //We build a map to cache the field paths. Avoids creating it everytime
  private val kcqlMap = settings.kcqls.groupBy(_.getSource).map { case (topic, kcqls) =>
    def buildFieldPath(field: Field) = {
      field -> (Option(field.getParentFields).map(_.toVector).getOrElse(Vector.empty[String]) :+ field.getName)
    }

    topic -> kcqls.map { kcql =>

      val fields = kcql.getFields().map(buildFieldPath)
      val ignoredFields = kcql.getIgnoredFields.map(_.toString).toSet

      val timestampField = Option(kcql.getTimestamp) match {
        case Some(Kcql.TIMESTAMP) => None
        case other => other
      }

      val tags = Option(kcql.getTags).map { tags =>
        tags.map { tag =>
          if (tag.getType == Tag.TagType.CONSTANT) tag -> Vector.empty
          else tag -> tag.getKey.split('.').toVector
        }
      }.getOrElse(Vector.empty)

      KcqlCache(kcql, fields, ignoredFields, tags, timestampField.map(_.split('.').toVector))
    }
  }

  def build(records: Iterable[SinkRecord]): BatchPoints = {

    def handleSinkRecord(record: SinkRecord): Seq[Point] = {

      Option(record.valueSchema()) match {
        case None =>
          //try to take it as string
          record.value() match {
            case _: java.util.Map[_, _] => buildPointFromMap(record)
            case _ => sys.error("For schemaless record only String and Map types are supported")
          }
        case Some(schema: Schema) =>
          schema.`type`() match {
            case Schema.Type.STRING => buildPointFromJson(record)
            case Schema.Type.STRUCT => buildPointFromStruct(record)
            case other => sys.error(s"$other schema is not supported")
          }
      }
    }

    val batchPoints = BatchPoints
      .database(settings.database)
      .retentionPolicy(settings.retentionPolicy)
      .consistency(settings.consistencyLevel)
      .build()

    records.foreach(r => handleSinkRecord(r).map(batchPoints.point))
    batchPoints
  }


  /**
    * Builds an InfluxDb [[Point]] from a [[java.util.Map]]
    *
    * @param record - An instance of [[SinkRecord]] with the payload expected to be [[java.util.Map]]
    * @return An instance [[Seq[Point]]. The reason for sequence is because we might have multiple KCQL on the same topic
    */
  private def buildPointFromMap(record: SinkRecord): Seq[Point] = {
    require(record.value() != null && record.value().getClass == classOf[java.util.HashMap[_, _]],
      "The SinkRecord payload should be of type java.util.Map[String, Any]")

    val map = record.value().asInstanceOf[java.util.Map[String, Any]]

    val kcqls = kcqlMap.getOrElse(record.topic(),
      throw new ConfigException(s"Topic '${record.topic()}' is missing KCQL mapping."))

    kcqls.map { k =>

      var tsUnit: Option[TimeUnit] = None
      val timestamp = k.timestampField.map { implicit fieldPath =>
        val tsRaw = ValuesExtractor.extract(map, fieldPath)
        TimestampValueCoerce(tsRaw)
      }.getOrElse {
        tsUnit = Some(TimeUnit.NANOSECONDS)
        nanoClock.getEpochNanos
      }

      val measurement = getMeasurement(k) { fieldPath => ValuesExtractor.extract(map, fieldPath) }
      implicit val builder = Point.measurement(measurement).time(timestamp, tsUnit.getOrElse(k.kcql.getTimestampUnit))

      buildPointFields(k,
        ValuesExtractor.extract(map, _),
        ValuesExtractor.extractAllFieldsCallback(map, k.IgnoredAndAliasFields, _)
      )

      k.tagsAndPaths.foreach { case (tag, path) =>
        tag.getType match {
          case Tag.TagType.CONSTANT =>
            builder.tag(tag.getKey, tag.getValue)

          case Tag.TagType.ALIAS =>
            Option(ValuesExtractor.extract(map, path)).foreach { v =>
              builder.tag(tag.getValue, v.toString)
            }

          case _ =>
            Option(ValuesExtractor.extract(map, path)).foreach { v =>
              builder.tag(tag.getKey, v.toString)
            }
        }
      }

      builder.build()
    }
  }


  /**
    * Builds an InfluxDb [[Point]] from a [[com.fasterxml.jackson.databind.node.ObjectNode]]
    *
    * @param record - An instance of [[SinkRecord]] with the payload expected to be [[com.fasterxml.jackson.databind.node.ObjectNode]]
    * @return An instance [[Seq[Point]]. The reason for sequence is because we might have multiple KCQL on the same topic
    */
  private def buildPointFromJson(record: SinkRecord): Seq[Point] = {
    require(record.value() != null && record.value().getClass == classOf[String], "The SinkRecord payload should be of type String")

    val json = JacksonJson.asJson(record.value().asInstanceOf[String])

    val kcqls = kcqlMap.getOrElse(record.topic(), throw new ConfigException(s"Topic '${record.topic()}' is missing KCQL mapping."))

    kcqls.map { k =>

      var tsUnit: Option[TimeUnit] = None
      val timestamp = k.timestampField.map { implicit fieldPath =>
        val tsRaw = ValuesExtractor.extract(json, fieldPath)
        TimestampValueCoerce(tsRaw)
      }.getOrElse {
        tsUnit = Some(TimeUnit.NANOSECONDS)
        nanoClock.getEpochNanos
      }

      val measurement = getMeasurement(k) { fieldPath => ValuesExtractor.extract(json, fieldPath) }
      implicit val builder = Point.measurement(measurement).time(timestamp, tsUnit.getOrElse(k.kcql.getTimestampUnit))

      buildPointFields(k,
        ValuesExtractor.extract(json, _),
        ValuesExtractor.extractAllFieldsCallback(json, k.IgnoredAndAliasFields, _)
      )

      k.tagsAndPaths.foreach { case (tag, path) =>
        tag.getType match {
          case Tag.TagType.CONSTANT =>
            builder.tag(tag.getKey, tag.getValue)

          case Tag.TagType.ALIAS =>
            Option(ValuesExtractor.extract(json, path)).foreach { v =>
              builder.tag(tag.getValue, v.toString)
            }

          case other =>
            Option(ValuesExtractor.extract(json, path)).foreach { v =>
              builder.tag(tag.getKey, v.toString)
            }
        }
      }

      builder.build()
    }
  }

  private def getMeasurement(k: KcqlCache)(thunk: Vector[String] => Any): String = {
    k.DynamicTarget
      .map { fieldPath =>
        Option(thunk(fieldPath)).map(_.toString).getOrElse(k.kcql.getTarget)
      }.getOrElse(k.kcql.getTarget)
  }

  /**
    * Builds an InfluxDb [[Point]] from a [[Struct]].This is when the payload of Kafka message is AVRO
    *
    * @param record - An instance of [[SinkRecord]] with the payload expected to be [[Struct]]
    * @return An instance [[Seq[Point]]. The reason for sequence is because we might have multiple KCQL on the same topic
    */
  private def buildPointFromStruct(record: SinkRecord): Seq[Point] = {
    require(record.value() != null && record.value().getClass == classOf[Struct], "The SinkRecord payload should be of type Struct")

    val struct = record.value().asInstanceOf[Struct]
    val kcqls = kcqlMap.getOrElse(record.topic(), throw new ConfigException(s"Topic '${record.topic()}' is missing KCQL mapping."))

    kcqls.map { k =>
      var tsUnit: Option[TimeUnit] = None
      val timestamp = k.timestampField.map { implicit fieldPath =>
        val tsRaw = ValuesExtractor.extract(struct, fieldPath)
        TimestampValueCoerce(tsRaw)
      }.getOrElse {
        tsUnit = Some(TimeUnit.NANOSECONDS)
        nanoClock.getEpochNanos
      }

      val measurement = getMeasurement(k) { fieldPath => ValuesExtractor.extract(struct, fieldPath) }
      implicit val builder = Point.measurement(measurement).time(timestamp, tsUnit.getOrElse(k.kcql.getTimestampUnit))

      buildPointFields(k,
        ValuesExtractor.extract(struct, _),
        ValuesExtractor.extractAllFieldsCallback(struct, k.IgnoredAndAliasFields, _)
      )

      k.tagsAndPaths.foreach { case (tag, path) =>
        tag.getType match {
          case Tag.TagType.CONSTANT =>
            builder.tag(tag.getKey, tag.getValue)

          case Tag.TagType.ALIAS =>
            Option(ValuesExtractor.extract(struct, path)).foreach { v =>
              builder.tag(tag.getValue, v.toString)
            }

          case other =>
            Option(ValuesExtractor.extract(struct, path)).foreach { v =>
              builder.tag(tag.getKey, v.toString)
            }
        }
      }

      builder.build()
    }
  }

  /**
    * Populates the fields for the [[Point.Builder]]
    *
    * @param kcqlCache      - Wraps the KCQL by caching a few values around the fields path
    * @param extractorFn    - A function accepting a field path and returns its value
    * @param getAllFieldsFn - Returns all the fields. Used when KCQL includes all fields via '*'
    */
  private def buildPointFields(kcqlCache: KcqlCache,
                               extractorFn: Vector[String] => Any,
                               getAllFieldsFn: ((String, Any) => Unit) => Unit)(implicit builder: Point.Builder) = {

    val populateFieldFn = (field: String, v: Any) => v match {
      case value: Long => builder.addField(field, value)
      case value: Int => builder.addField(field, value)
      case value: BigInt => builder.addField(field, value)
      case value: Byte => builder.addField(field, value)
      case value: Short => builder.addField(field, value)
      case value: Double => builder.addField(field, value)
      case value: Float => builder.addField(field, value)
      case value: Boolean => builder.addField(field, value)
      case value: java.math.BigDecimal => builder.addField(field, value)
      case value: BigDecimal => builder.addField(field, value.bigDecimal)
      case value: String => builder.addField(field, value)
      case value: java.util.Date => builder.addField(field, value.getTime)
      //we should never reach this since the extractor should not allow it
      case value => sys.error(s"Can't select field:'$field' because it leads to value:'$value' (${Option(value).map(_.getClass.getName).getOrElse("")})is not a valid type for InfluxDb.")
    }


    //ignore all the fields we should ignore
    kcqlCache.fieldsAndPaths.filter(p => !kcqlCache.ignoredFields.contains(p._1.toString))
      .foreach { case (field, path) =>
        if (field.getName == "*") {
          getAllFieldsFn({ case (f, value) => populateFieldFn(f, value) })
        }
        else {
          Option(extractorFn(path)).foreach(populateFieldFn(field.getAlias, _))
        }
      }

  }

}

/**
  * Builds a cache of fields path (fields to select, fields to ignore, tags and timestamp)
  *
  * @param kcql
  * @param fieldsAndPaths
  * @param ignoredFields
  * @param tagsAndPaths
  * @param timestampField
  */
case class KcqlCache(kcql: Kcql,
                     fieldsAndPaths: Seq[(Field, Vector[String])],
                     ignoredFields: Set[String],
                     tagsAndPaths: Seq[(Tag, Vector[String])],
                     timestampField: Option[Vector[String]]) {
  val IgnoredAndAliasFields: Set[String] = {
    val allIgnored = fieldsAndPaths
      .filter(p => p._1.getAlias != p._1.getName)
      .map(_._1)
      .foldLeft(ignoredFields) { case (acc, f) => acc + f.toString }
    allIgnored
  }

  val DynamicTarget = Option(kcql.getDynamicTarget).map(_.split('.').toVector)
}