/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.extractors

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import org.apache.kafka.connect.data.Schema

import java.util

/**
  * Extracts values from a Java map type
  */
object MapExtractor extends LazyLogging {

  private[extractors] def extractPathFromMap(
    map:       util.Map[_, _],
    fieldName: PartitionNamePath,
    schema:    Schema,
  ): Either[ExtractorError, String] =
    if (fieldName.hasTail) extractComplexType(map, fieldName, schema) else extractPrimitive(map, fieldName.head, schema)

  private def extractComplexType(
    map:       util.Map[_, _],
    fieldName: PartitionNamePath,
    schema:    Schema,
  ): Either[ExtractorError, String] = {

    val maybeSchema = Option(schema)

    val mapKey        = fieldName.head
    val maybeMapValue = Option(map.get(mapKey))

    (maybeMapValue, maybeSchema) match {
      case (None, _) =>
        ExtractorError(ExtractorErrorType.MissingValue).asLeft[String]
      case (Some(mapValue), Some(sch)) =>
        ComplexTypeExtractor.extractComplexType(mapValue, fieldName.tail, sch.valueSchema())
      case (Some(mapValue), None) =>
        WrappedComplexTypeExtractor.extractFromComplexType(mapValue, fieldName.tail)
    }
  }

  private def extractPrimitive(
    map:       util.Map[_, _],
    fieldName: String,
    mapSchema: Schema,
  ): Either[ExtractorError, String] =
    Option(mapSchema).flatMap(_.valueSchema().some) match {
      case Some(sch) => PrimitiveExtractor.extractPrimitiveValue(map.get(fieldName), sch)
      case None      => WrappedPrimitiveExtractor.extractFromPrimitive(map.get(fieldName))
    }

}
