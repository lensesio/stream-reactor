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
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Schema.Type._

object PrimitiveExtractor extends LazyLogging {

  private[extractors] def extractPrimitiveValue(value: Any, schema: Schema): Either[ExtractorError, String] = {
    schema.`type`() match {
      case INT8 => anyToEither(value)
      case INT16 => anyToEither(value)
      case INT32 => anyToEither(value)
      case INT64 => anyToEither(value)
      case FLOAT32 => anyToEither(value)
      case FLOAT64 => anyToEither(value)
      case BOOLEAN => anyToEither(value)
      case STRING => anyToEither(value)
      case BYTES => Option(value).fold(ExtractorError(ExtractorErrorType.MissingValue).asLeft[String]) {
        case byteVal: Array[Byte] => new String(byteVal).asRight[ExtractorError]
      }
      case other => logger.error("Non-primitive values not supported: " + other)
        ExtractorError(ExtractorErrorType.UnexpectedType).asLeft[String]
    }
  }

  private[extractors] def anyToEither(any: Any): Either[ExtractorError, String] =
    Option(any).fold(ExtractorError(ExtractorErrorType.MissingValue).asLeft[String])(_.toString.asRight[ExtractorError])

}
