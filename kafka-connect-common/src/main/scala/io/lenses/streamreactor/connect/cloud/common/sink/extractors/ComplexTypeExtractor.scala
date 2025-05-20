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
import org.apache.kafka.connect.data.Struct

import java.util

object ComplexTypeExtractor extends LazyLogging {

  private[extractors] def extractComplexType(
    value:     Any,
    fieldName: PartitionNamePath,
    schema:    Schema,
  ): Either[ExtractorError, String] =
    value match {
      case s: Struct         => StructExtractor.extractPathFromStruct(s, fieldName)
      case m: util.Map[_, _] => MapExtractor.extractPathFromMap(m, fieldName, schema)
      case a: util.List[_]   => ArrayExtractor.extractPathFromArray(a, fieldName, schema)
      case other => logger.error("Unexpected type in Map Extractor: " + other)
        ExtractorError(ExtractorErrorType.UnexpectedType).asLeft[String]
    }
}
