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
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath

/**
 * Extracts values from a Map.
 */
object WrappedMapExtractor {

  private[extractors] def extractPathFromMap(
    map:       java.util.Map[_, _],
    fieldName: PartitionNamePath,
  ): Either[ExtractorError, String] =
    if (fieldName.hasTail) extractComplexType(map, fieldName) else extractPrimitive(map, fieldName.head)

  private def extractComplexType(
    map:       java.util.Map[_, _],
    fieldName: PartitionNamePath,
  ): Either[ExtractorError, String] =
    Option(map.asInstanceOf[java.util.Map[Any, Any]]
      .get(fieldName.head))
      .fold(ExtractorError(ExtractorErrorType.MissingValue).asLeft[String])(
        WrappedComplexTypeExtractor.extractFromComplexType(_, fieldName.tail),
      )

  private def extractPrimitive(map: java.util.Map[_, _], head: String): Either[ExtractorError, String] =
    Option(map.get(head)).fold(ExtractorError(ExtractorErrorType.MissingValue).asLeft[String]) {
      wrappedPrimitive => WrappedPrimitiveExtractor.extractFromPrimitive(wrappedPrimitive)
    }

}
