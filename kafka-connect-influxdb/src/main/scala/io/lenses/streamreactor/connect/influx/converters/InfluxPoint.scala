/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.influx.converters

import io.lenses.streamreactor.connect.influx.NanoClock
import io.lenses.streamreactor.connect.influx.converters.SinkRecordParser.ParsedKeyValueSinkRecord
import io.lenses.streamreactor.connect.influx.converters.SinkRecordParser.ParsedSinkRecord
import io.lenses.streamreactor.connect.influx.helpers.Util
import io.lenses.streamreactor.connect.influx.writers.KcqlDetails
import io.lenses.streamreactor.connect.influx.writers.KcqlDetails.ConstantTag
import io.lenses.streamreactor.connect.influx.writers.KcqlDetails.DynamicTag
import io.lenses.streamreactor.connect.influx.writers.KcqlDetails.Path
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point

import java.time.Instant
import java.util.Date
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object InfluxPoint {

  def build(nanoClock: NanoClock)(record: ParsedKeyValueSinkRecord, details: KcqlDetails): Try[Point] =
    for {
      (timeUnit, timestamp) <- extractTimeMeasures(nanoClock, record, details)
      measurement = details.dynamicTarget
        .flatMap(record.field)
        .map(_.toString)
        .getOrElse(details.target)
      pointBuilder = Point.measurement(measurement).time(timestamp, timeUnit)
      point       <- addValuesAndTags(pointBuilder, record, details)
    } yield point

  private def addValuesAndTags(
    pointBuilder: Point,
    record:       ParsedKeyValueSinkRecord,
    details:      KcqlDetails,
  ): Try[Point] =
    details.NonIgnoredFields
      .flatMap {
        case (_, path, _) if path.equals(Path(Util.KEY_All_ELEMENTS)) =>
          record.keyFields(ignored = details.IgnoredAndAliasFields).map {
            case (name, value) => name -> Some(value)
          }
        case (field, _, _) if field.value == "*" =>
          record.valueFields(ignored = details.IgnoredAndAliasFields).map {
            case (name, value) => name -> Some(value)
          }
        case (field, path, _) => (field.value -> record.field(path)) :: Nil
      }
      .foldLeft(Try(pointBuilder))((builder, field) =>
        builder.flatMap(b =>
          field match {
            case (key, Some(value)) => writeField(b)(key, value)
            case (key, _) =>
              Failure(
                new IllegalArgumentException(
                  s"Property $key is referenced but no value could be found .",
                ),
              )
          },
        ),
      )
      .flatMap(builder =>
        details.tags
          .map {
            case ConstantTag(key, value) => (key.value, Some(value))
            case DynamicTag(name, path) =>
              (name.value, record.field(path).map(_.toString))
          }
          .foldLeft(Try(builder)) { (builder, tag) =>
            tag match {
              case (key, Some(value)) => builder.map(_.addTag(key, value))
              case (_, None)          => builder
            }
          },
      )

  def toWritePrecision(timestampUnit: TimeUnit): Option[WritePrecision] = Option(timestampUnit).collect {
    case TimeUnit.NANOSECONDS  => WritePrecision.NS
    case TimeUnit.MICROSECONDS => WritePrecision.US
    case TimeUnit.MILLISECONDS => WritePrecision.MS
    case TimeUnit.SECONDS      => WritePrecision.S
  }

  private def extractTimeMeasures(
    nanoClock: NanoClock,
    record:    ParsedSinkRecord,
    details:   KcqlDetails,
  ): Try[(WritePrecision, Long)] = {
    {
      for {
        writePrecision <- toWritePrecision(details.timestampUnit)
        field          <- details.timestampField
        pathFromRecord <- record.field(field).map(coerceTimeStamp(_, field.value))
      } yield pathFromRecord.map(p => writePrecision -> p)
    }
  }.getOrElse(Try(WritePrecision.NS -> nanoClock.getEpochNanos))

  private[influx] def coerceTimeStamp(
    value:     Any,
    fieldPath: Iterable[String],
  ): Try[Long] =
    value match {
      case b: Byte  => Try(b.toLong)
      case s: Short => Try(s.toLong)
      case i: Int   => Try(i.toLong)
      case l: Long => Try(l)
      case s: String =>
        Try(Instant.parse(s).toEpochMilli).transform(
          Try(_),
          e =>
            Failure(
              new IllegalArgumentException(
                s"$s is not a valid format for timestamp, expected 'yyyy-MM-DDTHH:mm:ss.SSSZ'",
                e,
              ),
            ),
        )
      case d: Date => Try(d.toInstant.toEpochMilli)
      /* Assume Unix timestamps in seconds with double precision, coerce to Long with milliseconds precision */
      case d: Double => Try((d * 1e3).toLong)
      case other =>
        Failure(
          new IllegalArgumentException(
            s"Invalid value for field:${fieldPath.mkString(".")}.Value '$other' is not a valid field for the timestamp",
          ),
        )
    }

  def writeField(builder: Point)(field: String, v: Any): Try[Point] = v match {
    case value: Long                 => Try(builder.addField(field, value))
    case value: Int                  => Try(builder.addField(field, value.toLong))
    case value: BigInt               => Try(builder.addField(field, value))
    case value: Byte                 => Try(builder.addField(field, value.toLong))
    case value: Short                => Try(builder.addField(field, value.toLong))
    case value: Double               => Try(builder.addField(field, value))
    case value: Float                => Try(builder.addField(field, value.toDouble))
    case value: Boolean              => Try(builder.addField(field, value))
    case value: java.math.BigDecimal => Try(builder.addField(field, value))
    case value: BigDecimal           => Try(builder.addField(field, value.bigDecimal))
    case value: String               => Try(builder.addField(field, value))
    case value: java.util.Date       => Try(builder.addField(field, value.getTime))
    case value: java.util.List[_]    => flattenArray(builder)(field, value)
    case value =>
      Failure(
        new RuntimeException(
          s"Can't select field:'$field' because it leads to value:'$value' (${Option(
            value,
          ).map(_.getClass.getName).getOrElse("")})is not a valid type for InfluxDb.",
        ),
      )
  }

  /**
    * Flatten an array writing each element as a new field with the following convention:
    * "name": ["a", "b", "c"] => name0 = "a", name1 = "b", name3 = "c"
    */
  private def flattenArray(builder: Point)(field: String, value: java.util.List[_]) = {
    val res = value.asScala.zipWithIndex.map {
      case (el, i) => writeField(builder)(field + i, el)
    }
    // return first failure
    res.collectFirst({
      case Failure(x) => Failure(x)
    }).getOrElse(Success(builder))
  }
}
