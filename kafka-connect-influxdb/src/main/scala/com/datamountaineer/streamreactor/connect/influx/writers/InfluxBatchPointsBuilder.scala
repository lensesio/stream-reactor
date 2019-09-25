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

import com.datamountaineer.kcql.{Field, Kcql, Tag}
import com.datamountaineer.streamreactor.connect.influx.NanoClock
import com.datamountaineer.streamreactor.connect.influx.config.InfluxSettings
import com.datamountaineer.streamreactor.connect.influx.converters.{InfluxPoint, SinkRecordParser}
import com.datamountaineer.streamreactor.connect.influx.helpers.Util
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

  private def buildFieldPath(field: Field): (Field, Vector[String]) = {
    field -> (Option(field.getParentFields).map(_.toVector).getOrElse(Vector.empty[String]) :+ field.getName)
  }

  private val kcqlMap: Map[String, Seq[KcqlCache]] = settings.kcqls.groupBy(_.getSource).map { case (topic, kcqls) =>
    topic -> kcqls.map { kcql =>
      val fields = kcql.getFields.map(buildFieldPath)
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
              .map { kcqls =>
                kcqls.map(kcql => InfluxPoint.build(nanoClock)(parsedRecord, kcql))
              }
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
  val nonIgnoredFields = fieldsAndPaths.filter{
    case (field , _) => !ignoredFields.contains(field.toString)
  }
}
