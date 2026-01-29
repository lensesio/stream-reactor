/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.elastic6.indexname

import cats.implicits.catsSyntaxEitherId
import cats.implicits.toBifunctorOps
import io.lenses.kcql.Kcql
import io.lenses.kcql.targettype.HeaderTargetType
import io.lenses.kcql.targettype.KeyTargetType
import io.lenses.kcql.targettype.StaticTargetType
import io.lenses.kcql.targettype.TargetType
import io.lenses.kcql.targettype.ValueTargetType
import io.lenses.streamreactor.common.utils.CyclopsToScalaEither.convertToScalaEither
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.HeaderToSinkDataConverter
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.ValueToSinkDataConverter
import io.lenses.streamreactor.connect.cloud.common.sink.extractors.QuotedPathSplitter
import io.lenses.streamreactor.connect.cloud.common.sink.extractors.SinkDataExtractor
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord

import scala.jdk.OptionConverters.RichOptional

/**
  * Creates the index for the given KCQL configuration.
  */
object CreateIndex {
  def getIndexNameForAutoCreate(kcql: Kcql): Either[IllegalArgumentException, String] =
    for {
      targetType: TargetType <- convertToScalaEither(kcql.getTargetType)
      index <- {
        Option(targetType).collect {
          case targetType: StaticTargetType =>
            targetType.getTarget
        }.toRight(new IllegalArgumentException(
          "AutoCreate can not be used in conjunction with targets for keys, values or headers",
        ))
      }
    } yield {
      createIndexName(kcql, index)
    }

  def getIndexName(kcql: Kcql, r: SinkRecord): Either[IllegalArgumentException, String] =
    for {
      targetType: TargetType <- convertToScalaEither(kcql.getTargetType)
      index <- {
        extractTargetFromMessage(r, targetType)
      }.leftMap(e => new IllegalArgumentException(e))
    } yield {
      createIndexName(kcql, index)
    }

  private def extractTargetFromMessage(r: SinkRecord, targetType: TargetType) =
    targetType match {
      case headerTargetType: HeaderTargetType =>
        val headerPath: Array[String] = QuotedPathSplitter.splitPath(headerTargetType.getName, "'")
        extractHeader(r, headerPath)

      case keyTargetType: KeyTargetType =>
        val splitPath = keyTargetType.getPath.toOptional.toScala.map(QuotedPathSplitter.splitPath(_, "'"))
        extractValue(splitPath, r.key(), Option(r.keySchema()))

      case valueTargetType: ValueTargetType =>
        val splitPath = valueTargetType.getPath.toOptional.toScala.map(QuotedPathSplitter.splitPath(_, "'"))
        extractValue(splitPath, r.value(), Option(r.valueSchema()))

      case targetType: StaticTargetType =>
        targetType.getTarget.asRight
    }

  private def extractHeader(r: SinkRecord, headerPath: Array[String]) = {
    val sinkData: SinkData = HeaderToSinkDataConverter(r, headerPath.head)
    SinkDataExtractor.extractPathFromSinkData(sinkData)(
      Option.when(headerPath.tail.nonEmpty)(PartitionNamePath(headerPath.tail.toIndexedSeq: _*)),
    )
  }

  private def extractValue(splitPath: Option[Array[String]], value: AnyRef, maybeSchema: Option[Schema]) = {
    val sinkData: SinkData = ValueToSinkDataConverter(value, maybeSchema)
    val partitionNamePath = splitPath.map(path => PartitionNamePath(path.toIndexedSeq: _*))
    SinkDataExtractor.extractPathFromSinkData(sinkData)(partitionNamePath)
  }

  private def createIndexName(kcql: Kcql, index: String) =
    Option(kcql.getIndexSuffix).fold(index) { indexNameSuffix =>
      s"$index${CustomIndexName.parseIndexName(indexNameSuffix)}"
    }

}
