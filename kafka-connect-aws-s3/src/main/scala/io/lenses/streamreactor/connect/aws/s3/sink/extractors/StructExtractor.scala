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
import io.lenses.streamreactor.connect.aws.s3.model.PartitionNamePath
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.PrimitiveExtractor.anyToEither
import org.apache.kafka.connect.data.Schema.Type._
import org.apache.kafka.connect.data.Struct

/**
  * Extracts value from a Struct type
  */
object StructExtractor extends LazyLogging {

  private[extractors] def extractPathFromStruct(struct: Struct, fieldName: PartitionNamePath): Either[ExtractorError, String] = {
    if (fieldName.hasTail) extractComplexType(struct, fieldName) else extractPrimitive(struct, fieldName.head)
  }

  private def extractPrimitive(struct: Struct, fieldName: String): Either[ExtractorError, String] = {
    Option(struct.schema().field(fieldName))
      .fold(ExtractorError(ExtractorErrorType.MissingValue).asLeft[String]) {
        _.schema().`type`() match {
          case INT8 => anyToEither(struct.getInt8(fieldName))
          case INT16 => anyToEither(struct.getInt16(fieldName))
          case INT32 => anyToEither(struct.getInt32(fieldName))
          case INT64 => anyToEither(struct.getInt64(fieldName))
          case FLOAT32 => anyToEither(struct.getFloat32(fieldName))
          case FLOAT64 => anyToEither(struct.getFloat64(fieldName))
          case BOOLEAN => anyToEither(struct.getBoolean(fieldName))
          case STRING => anyToEither(struct.getString(fieldName))
          case BYTES => Option(struct.getBytes(fieldName))
            .fold(ExtractorError(ExtractorErrorType.MissingValue).asLeft[String])(
              byteVal => new String(byteVal).asRight[ExtractorError]
            )
          case other => logger.error("Non-primitive values not supported: " + other)
            ExtractorError(ExtractorErrorType.UnexpectedType).asLeft[String]
        }

      }
  }

  private def extractComplexType(struct: Struct, fieldName: PartitionNamePath): Either[ExtractorError, String] = {
    Option(struct.schema().field(fieldName.head))
      .fold(ExtractorError(ExtractorErrorType.MissingValue).asLeft[String]) {
        _.schema().`type`() match {
          case STRUCT => extractPathFromStruct(struct.getStruct(fieldName.head), fieldName.tail)
          case MAP => MapExtractor.extractPathFromMap(
            struct.getMap(fieldName.head), fieldName.tail, struct.schema().field(fieldName.head).schema()
          )
          case ARRAY => ArrayExtractor.extractPathFromArray(
            struct.getArray(fieldName.head), fieldName.tail, struct.schema().field(fieldName.head).schema()
          )
        }
      }
  }

}
