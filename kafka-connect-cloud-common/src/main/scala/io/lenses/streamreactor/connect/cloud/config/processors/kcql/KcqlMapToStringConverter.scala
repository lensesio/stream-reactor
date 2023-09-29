/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.config.processors.kcql

import cats.implicits.catsSyntaxEitherId

/**
  * Converts KCQL properties to a KCQL String for use by the connector configuration in the following format.
  */
object KcqlMapToStringConverter {

  /**
    * Given a Map of KCQL Properties Map[KcqlProp, String], this builds and returns a Kcql string.
    *
    * {{{
    * INSERT INTO bucketAddress:pathPrefix
    * SELECT *
    * FROM kafka-topic
    * [PARTITIONBY (partition[, partition] ...)]
    * [STOREAS storage_format]
    * [WITHPARTITIONER = partitioner]
    * [WITH_FLUSH_SIZE = flush_size]
    * [WITH_FLUSH_INTERVAL = flush_interval]
    * [WITH_FLUSH_COUNT = flush_count]
    * }}}
    */
  def convert(kcqlAsProperties: Map[KcqlProp, String]): Either[String, String] = {

    def nonOptional(propName: KcqlProp): Either[String, String] =
      optional(propName).fold(_.asLeft,
                              {
                                case Some(value) => value.asRight
                                case None        => "no value for nonOptional".asLeft
                              },
      )

    def optional(propName: KcqlProp): Either[String, Option[String]] =
      kcqlAsProperties.get(propName) match {
        case Some(value: String) => Some(value).asRight
        case None => None.asRight
      }

    for {
      target         <- nonOptional(KcqlProp.Target)
      source         <- nonOptional(KcqlProp.Source)
      batchSize      <- optional(KcqlProp.BatchSize)
      partitions     <- optional(KcqlProp.Partitions)
      format         <- optional(KcqlProp.Format)
      partitioner    <- optional(KcqlProp.Partitioner)
      flush_size     <- optional(KcqlProp.FlushSize)
      flush_interval <- optional(KcqlProp.FlushInterval)
      flush_count    <- optional(KcqlProp.FlushCount)
      limit          <- optional(KcqlProp.Limit)
    } yield joinKcql(
      generateInsert(target, source),
      generateBatchSize(batchSize),
      generatePartitions(partitions),
      generateFormat(format),
      generatePartitioner(partitioner),
      generateFlushSize(flush_size, flush_interval, flush_count),
      generateLimit(limit),
    )
  }

  private def generateInsert(target: String, source: String): String =
    s"INSERT INTO `$target` SELECT * FROM `$source`"

  private def generatePartitions(partitions: Option[String]): Option[String] =
    partitions.map("PARTITIONBY " + _)

  private def generateBatchSize(batchSize: Option[String]): Option[String] = batchSize.map(s"BATCH " + _)

  private def generateLimit(limit: Option[String]): Option[String] = limit.map(s"LIMIT " + _)

  private def generateFormat(format: Option[String]): Option[String] = format.map("STOREAS `" + _ + "`")

  private def generatePartitioner(partitioner: Option[String]): Option[String] =
    partitioner.map("WITHPARTITIONER = " + _)

  private def generateFlushSize(
    flush_size:     Option[String],
    flush_interval: Option[String],
    flush_count:    Option[String],
  ): Option[String] =
    Option(
      Seq(
        flush_size.map("WITH_FLUSH_SIZE = " + _),
        flush_interval.map("WITH_FLUSH_INTERVAL = " + _),
        flush_count.map("WITH_FLUSH_COUNT = " + _),
      ).filter(_.nonEmpty).flatten.mkString(" "),
    )

  private def joinKcql(
    insert:      String,
    batchSize:   Option[String],
    partitions:  Option[String],
    format:      Option[String],
    partitioner: Option[String],
    flush:       Option[String],
    limit:       Option[String],
  ) =
    Seq(Some(insert), batchSize, partitions, format, partitioner, flush, limit).filter(_.nonEmpty).flatten.mkString(" ")

}
