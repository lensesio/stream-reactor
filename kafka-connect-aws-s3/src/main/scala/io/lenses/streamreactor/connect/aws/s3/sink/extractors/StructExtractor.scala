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

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.PartitionNamePath
import org.apache.kafka.connect.data.Schema.Type._
import org.apache.kafka.connect.data.Struct
import io.lenses.streamreactor.connect.aws.s3.sink.conversion.OptionConvert.convertToOption

object StructExtractor extends LazyLogging {

  def extractPathFromStruct(struct: Struct, fieldName: PartitionNamePath): Option[String] = {
    if (fieldName.hasTail) extractComplexType(struct, fieldName) else extractPrimitive(struct, fieldName.head)
  }

  private def extractPrimitive(struct: Struct, fieldName: String) = {
    Option(struct.schema().field(fieldName))
      .fold(Option.empty[String]) {
        _.schema().`type`() match {
          case INT8 => convertToOption(struct.getInt8(fieldName))
          case INT16 => convertToOption(struct.getInt16(fieldName))
          case INT32 => convertToOption(struct.getInt32(fieldName))
          case INT64 => convertToOption(struct.getInt64(fieldName))
          case FLOAT32 => convertToOption(struct.getFloat32(fieldName))
          case FLOAT64 => convertToOption(struct.getFloat64(fieldName))
          case BOOLEAN => convertToOption(struct.getBoolean(fieldName))
          case STRING => convertToOption(struct.getString(fieldName))
          case BYTES => Option(struct.getBytes(fieldName)).fold(Option.empty[String])(byteVal => Some(new String(byteVal)))
          case other => logger.error("Non-primitive values not supported: " + other)
            throw new IllegalArgumentException("Non-primitive values not supported: " + other)
        }

      }
  }

  private def extractComplexType(struct: Struct, fieldName: PartitionNamePath) = {
    Option(struct.schema().field(fieldName.head))
      .fold(Option.empty[String]) {
        _.schema().`type`() match {
          case STRUCT => extractPathFromStruct(struct.getStruct(fieldName.head), fieldName.tail)
          case MAP => MapExtractor.extractPathFromMap(
            struct.getMap(fieldName.head), fieldName.tail, struct.schema().field(fieldName.head).schema()
          )
          case ARRAY => convertToOption(struct.getString(fieldName.toString))
        }
      }
  }

}
