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
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.ExtractorErrorType.IncorrectIndexType

object ArrayIndexUtil extends LazyLogging {

  private[extractors] def getArrayIndex(fieldName: String): Either[ExtractorError, Int] = {
    try {
      fieldName.toInt.asRight[ExtractorError]
    } catch {
      case _: NumberFormatException =>
        logger.error("Incorrect index type for Array type, expected only numeric characters")
        ExtractorError(IncorrectIndexType).asLeft
    }
  }
}
