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

import enumeratum._
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.ExtractorErrorType.{FieldNameNotSpecified, IncorrectIndexType, MissingValue, UnexpectedType}

sealed trait ExtractorErrorType extends EnumEntry

object ExtractorErrorType extends Enum[ExtractorErrorType] {

  val values = findValues

  case object MissingValue extends ExtractorErrorType

  case object UnexpectedType extends ExtractorErrorType

  case object FieldNameNotSpecified extends ExtractorErrorType

  case object IncorrectIndexType extends ExtractorErrorType
}

final case class ExtractorError(extractorErrorType: ExtractorErrorType,
                                cause: Throwable = None.orNull)
  extends Exception(extractorErrorType.toString, cause) {


}

object ExtractorErrorAdaptor {

  def adaptErrorResponse(either: Either[ExtractorError, String]): Option[String] = {
    either match {
      case Left(ExtractorError(MissingValue, _)) => None
      case Left(err@ExtractorError(UnexpectedType, _)) => throw err
      case Left(err@ExtractorError(FieldNameNotSpecified, _)) => throw err
      case Left(err@ExtractorError(IncorrectIndexType, _)) => throw err
      case Right(value) => Some(value)
    }
  }
}

