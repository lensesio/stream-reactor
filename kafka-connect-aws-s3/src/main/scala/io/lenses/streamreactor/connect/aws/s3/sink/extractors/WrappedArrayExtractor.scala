/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink.extractors

import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.model.{PartitionNamePath, SinkData}
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.ArrayIndexUtil.getArrayIndex

object WrappedArrayExtractor {

  private[extractors] def extractPathFromArray(arrs: Seq[SinkData], fieldName: PartitionNamePath): Either[ExtractorError, String] = {
    if (fieldName.hasTail) extractComplexType(arrs, fieldName) else extractPrimitive(arrs, fieldName.head)
  }

  private def extractComplexType(arrs: Seq[SinkData], fieldName: PartitionNamePath): Either[ExtractorError, String] = {
    getArrayIndex(fieldName.head) match {
      case Left(error) => error.asLeft[String]
      case Right(index) => arrs.lift(index).fold(ExtractorError(ExtractorErrorType.MissingValue).asLeft[String])(WrappedComplexTypeExtractor.extractFromComplexType(_, fieldName.tail))
    }
  }

  private def extractPrimitive(arrs: Seq[SinkData], head: String): Either[ExtractorError, String] = {
    getArrayIndex(head) match {
      case Left(error) => error.asLeft[String]
      case Right(index) => arrs.lift(index).fold(ExtractorError(ExtractorErrorType.MissingValue).asLeft[String]) {
        wrappedPrimitive => WrappedPrimitiveExtractor.extractFromPrimitive(wrappedPrimitive)
      }
    }
  }

}
