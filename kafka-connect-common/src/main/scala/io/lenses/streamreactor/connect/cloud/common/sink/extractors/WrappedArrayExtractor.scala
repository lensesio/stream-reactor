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
import ArrayIndexUtil.getArrayIndex
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath

object WrappedArrayExtractor {

  private[extractors] def extractPathFromArray(
    arrs:      java.util.List[_],
    fieldName: PartitionNamePath,
  ): Either[ExtractorError, String] =
    getArrayIndex(fieldName.head) match {
      case Left(error) => error.asLeft[String]
      case Right(index) =>
        Option(arrs.get(index)).fold(ExtractorError(ExtractorErrorType.MissingValue).asLeft[String]) { value =>
          if (fieldName.hasTail) WrappedComplexTypeExtractor.extractFromComplexType(value, fieldName.tail)
          else WrappedPrimitiveExtractor.extractFromPrimitive(value)
        }
    }
}
