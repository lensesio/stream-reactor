/*
 * Copyright 2017-2024 Lenses.io Ltd
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

object WrappedPrimitiveExtractor extends LazyLogging {

  private[extractors] def extractFromPrimitive(wrappedPrimitive: Any): Either[ExtractorError, String] =
    wrappedPrimitive match {
      case b:     Byte    => b.toString.asRight[ExtractorError]
      case b:     Boolean => b.toString.asRight[ExtractorError]
      case s:     Short   => s.toString.asRight[ExtractorError]
      case i:     Int     => i.toString.asRight[ExtractorError]
      case l:     Long    => l.toString.asRight[ExtractorError]
      case f:     Float   => f.toString.asRight[ExtractorError]
      case d:     Double  => d.toString.asRight[ExtractorError]
      case str:   String => str.asRight[ExtractorError]
      case array: Array[_] if array.isInstanceOf[Array[Byte]] =>
        new String(array.asInstanceOf[Array[Byte]]).asRight[ExtractorError]
      case other =>
        logger.error(s"Unable to represent a complex object as a string value ${other.getClass.getCanonicalName}")
        ExtractorError(ExtractorErrorType.UnexpectedType).asLeft[String]
    }

}
