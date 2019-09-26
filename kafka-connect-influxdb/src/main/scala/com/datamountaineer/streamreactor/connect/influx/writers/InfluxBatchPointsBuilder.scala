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

import com.datamountaineer.kcql.{Kcql, Tag}
import com.datamountaineer.streamreactor.connect.influx.NanoClock
import com.datamountaineer.streamreactor.connect.influx.config.InfluxSettings
import com.datamountaineer.streamreactor.connect.influx.converters.{InfluxPoint, SinkRecordParser}
import com.datamountaineer.streamreactor.connect.influx.helpers.Util
import com.datamountaineer.streamreactor.connect.influx.writers.KcqlDetails._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkRecord
import org.influxdb.dto.{BatchPoints, Point}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}


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
  private val kcqlMap: Map[String, Seq[KcqlDetails]] = settings.kcqls.groupBy(_.getSource).map { case (topic, kcqls) =>
    topic -> kcqls.map { kcql =>
      val timestampField = (Option(kcql.getTimestamp) match {
        case Some(Kcql.TIMESTAMP) => None
        case other => other
      }).map(_.split('.')).map(_.toSeq)

      val fields: Seq[(FieldName, Path, Option[Alias])] = kcql.getFields.toList.map { field =>
        (
          FieldName(Option(field.getAlias).getOrElse(field.getName)),
          Path(Option(field.getParentFields).map(_.toList).getOrElse(List.empty[String]) ::: field.getName :: Nil),
          Option(field.getAlias).filter(_ != field.getName).map(Alias)
        )
      }

      val ignores = kcql.getIgnoredFields.toList.map(field =>
        (FieldName(field.getName), Path(Option(field.getParentFields).map(_.toList).getOrElse(List.empty[String]) ::: field.getName :: Nil), Option(field.getAlias).map(Alias))
      ).toSet

      val tags = Option(kcql.getTags).toList.flatten.map { tag =>
        tag.getType match {
          case Tag.TagType.ALIAS => DynamicTag(FieldName(tag.getValue),Path(tag.getKey.split('.').toList))
          case Tag.TagType.CONSTANT => ConstantTag(FieldName(tag.getKey), tag.getValue)
          case Tag.TagType.DEFAULT => DynamicTag(FieldName(tag.getKey), Path(tag.getKey.split('.').toList))
        }
      }

      val dynamicTarget = Option(kcql.getDynamicTarget).map(_.split('.').toVector)
      val target = kcql.getTarget

      KcqlDetails(fields, ignores, tags, dynamicTarget.map(Path), target, timestampField.map(Path), kcql.getTimestampUnit)
    }
  }

  def build(records: Iterable[SinkRecord]): Try[BatchPoints] = {
    val batchPoints = BatchPoints
      .database(settings.database)
      .retentionPolicy(settings.retentionPolicy)
      .consistency(settings.consistencyLevel)
      .build()

    records
      .map { record =>
        SinkRecordParser
          .build(record)
          .flatMap { parsedRecord =>
            kcqlMap
              .get(record.topic())
              .map { kcqls => kcqls.map(kcql => InfluxPoint.build(nanoClock)(parsedRecord, kcql)) }
              .map(Success(_))
              .getOrElse(Failure(new ConfigException(s"Topic '${record.topic()}' is missing KCQL mapping.")))
          }
          .flatMap(_.foldLeft(Try(Seq.empty[Point]))(Util.shortCircuitOnFailure))
      }
      .foldLeft(Try(Seq.empty[Seq[Point]]))(Util.shortCircuitOnFailure)
      .map(_.flatten)
      .map { points =>
        points.foreach(batchPoints.point)
        batchPoints
      }
  }
}

/**
  * Builds a cache of fields path (fields to select, fields to ignore, tags and timestamp)
  *
  * @param kcql
  * @param fieldsAndPaths
  * @param ignoredFields
  * @param tags
  * @param timestampField
  */
case class KcqlDetails(fields: Seq[(FieldName, Path, Option[Alias])],
                       ignoredFields: Set[(FieldName, Path, Option[Alias])],
                       tags: Seq[ParsedTag],
                       dynamicTarget: Option[Path],
                       target: String,
                       timestampField: Option[Path],
                       timestampUnit: TimeUnit
                      ) {
  val IgnoredAndAliasFields: Set[Path] =
    fields
      .filter {
        case (_, _, Some(_)) => true
        case (_, _, None) => false
      }
      .map {
        case (_, path, _) => path
      }.toSet ++ ignoredFields.map { case (_, path, _) => path
    }

  val NonIgnoredFields: Seq[(FieldName, Path, Option[Alias])] = fields
    .filterNot {
      case (_, path, _) => ignoredFields.exists(_._2 == path)
    }
}

object KcqlDetails {

  case class Path(value: Seq[String]) {
    def equals(path: Path): Boolean = Util.caseInsensitiveComparison(value, path.value)
  }

  case class FieldName(value: String) extends AnyVal

  case class Alias(value: String) extends AnyVal

  sealed trait ParsedTag {
    def name: FieldName
  }

  case class ConstantTag(name: FieldName, value: String) extends ParsedTag

  case class DynamicTag(name: FieldName, path: Path) extends ParsedTag

}
