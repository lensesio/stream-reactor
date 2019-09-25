package com.datamountaineer.streamreactor.connect.influx.converters

import java.time.Instant
import java.util.Date
import java.util.concurrent.TimeUnit

import com.datamountaineer.kcql.Tag
import com.datamountaineer.streamreactor.connect.influx.NanoClock
import com.datamountaineer.streamreactor.connect.influx.converters.SinkRecordParser.ParsedSinkRecord
import com.datamountaineer.streamreactor.connect.influx.writers.KcqlCache
import org.influxdb.dto.Point

import scala.util.{Failure, Try}

object InfluxPoint {

  def build(nanoClock: NanoClock)(record: ParsedSinkRecord, details: KcqlCache): Try[Point] = {
    for {
      (timeUnit, timestamp) <- extractTimeMeasures(nanoClock, record, details)
      measurement = details.DynamicTarget.flatMap { fieldPath => record.field(fieldPath).map(_.toString) }.getOrElse(details.kcql.getTarget)
      pointBuilder = Point.measurement(measurement).time(timestamp, timeUnit)
      point <- addValuesAndTags(pointBuilder, record, details)
    } yield point.build()
  }

  private def addValuesAndTags(pointBuilder: Point.Builder, record: ParsedSinkRecord, details: KcqlCache): Try[Point.Builder] = {
    details.nonIgnoredFields.flatMap {
      case (field, _) if field.getName == "*" => record.valueFields(ignored = details.IgnoredAndAliasFields).map { case (name, value) => name -> Some(value) }
      case (field, path) => (field.getAlias -> record.field(path)) :: Nil
    }.foldLeft(Try(pointBuilder))((builder, field) =>
      builder.flatMap(b => field match {
        case (key, Some(value)) => writeField(b)(key, value)
        case (key, _) => ???
      })
    ).flatMap(builder =>
      details
        .tagsAndPaths
        .flatMap {
          case (tag, v) => Option(tag).flatMap(t => Option(t.getKey)).map(_ => tag -> v).toSeq //TODO: Improve error handling
        }.map {
        case (tag, _) if tag.getType == Tag.TagType.CONSTANT => (tag.getKey, Option(tag.getValue))
        case (tag, path) if tag.getType == Tag.TagType.ALIAS => (tag.getValue, record.field(path).map(_.toString))
        case (tag, path) => (tag.getKey, record.field(path).map(_.toString))
      }.foldLeft(Try(builder))((builder, tag) => {
        tag match {
          case (key, Some(value)) => builder.map(_.tag(key, value))
          case (key, None) => builder
        }
      }))
  }

  private def extractTimeMeasures(nanoClock: NanoClock, record: ParsedSinkRecord, details: KcqlCache): Try[(TimeUnit, Long)] = {
    details
      .timestampField
      .flatMap(record.field)
      .map(
        value => coerceTimeStamp(value, details.timestampField.toList.flatten).map(details.kcql.getTimestampUnit -> _)
      )
      .getOrElse(Try(TimeUnit.NANOSECONDS -> nanoClock.getEpochNanos))
  }

  private[influx] def coerceTimeStamp(value: Any, fieldPath: Traversable[String]): Try[Long] = {
    value match {
      case b: Byte => Try(b.toLong)
      case s: Short => Try(s.toLong)
      case i: Int => Try(i.toLong)
      case l: Long => Try(l)
      case s: String => Try(Instant.parse(s).toEpochMilli).transform(Try(_), e => Failure(new IllegalArgumentException(s"$s is not a valid format for timestamp, expected 'yyyy-MM-DDTHH:mm:ss.SSSZ'", e)))
      case d: Date => Try(d.toInstant.toEpochMilli)
      /* Assume Unix timestamps in seconds with double precision, coerce to Long with milliseconds precision */
      case d: Double => Try((d * 1E3).toLong)
      case other => Failure(new IllegalArgumentException(s"Invalid value for field:${fieldPath.mkString(".")}.Value '$other' is not a valid field for the timestamp"))
    }
  }

  def writeField(builder: Point.Builder)(field: String, v: Any): Try[Point.Builder] = v match {
    case value: Long => Try(builder.addField(field, value))
    case value: Int => Try(builder.addField(field, value))
    case value: BigInt => Try(builder.addField(field, value))
    case value: Byte => Try(builder.addField(field, value))
    case value: Short => Try(builder.addField(field, value))
    case value: Double => Try(builder.addField(field, value))
    case value: Float => Try(builder.addField(field, value))
    case value: Boolean => Try(builder.addField(field, value))
    case value: java.math.BigDecimal => Try(builder.addField(field, value))
    case value: BigDecimal => Try(builder.addField(field, value.bigDecimal))
    case value: String => Try(builder.addField(field, value))
    case value: java.util.Date => Try(builder.addField(field, value.getTime))
    case value => Failure(new RuntimeException(s"Can't select field:'$field' because it leads to value:'$value' (${Option(value).map(_.getClass.getName).getOrElse("")})is not a valid type for InfluxDb."))
  }
}