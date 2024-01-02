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
import WrappedMapExtractor.extractPathFromMap
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import org.apache.kafka.connect.data.Struct

object WrappedComplexTypeExtractor extends LazyLogging {

  private[extractors] def extractFromComplexType(
    wrappedComplexType: Any,
    fieldName:          PartitionNamePath,
  ): Either[ExtractorError, String] =
    wrappedComplexType match {
      case struct: Struct              => StructExtractor.extractPathFromStruct(struct, fieldName)
      case map:    java.util.Map[_, _] => extractPathFromMap(map, fieldName)
      case array:  java.util.List[_]   => WrappedArrayExtractor.extractPathFromArray(array, fieldName)
      case other =>
        logger.error(s"Unable to represent a complex object as a string value ${other.getClass.getCanonicalName}")
        ExtractorError(ExtractorErrorType.UnexpectedType).asLeft[String]
    }

}
