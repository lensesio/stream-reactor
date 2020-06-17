/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.formats

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.data.Schema.Type._
import org.apache.kafka.connect.data.Struct

object StructValueLookup extends LazyLogging {

  /**
    * Returns the value of a struct as a String for text output
    */
  def lookupFieldValueFromStruct(struct: Struct)(fieldName: String): Option[String] = {
    Option(struct.schema().field(fieldName))
      .fold(Option.empty[String]) {
        _.schema().`type`() match {
          case INT8 => Some(struct.getInt8(fieldName).toString)
          case INT16 => Some(struct.getInt16(fieldName).toString)
          case INT32 => Some(struct.getInt32(fieldName).toString)
          case INT64 => Some(struct.getInt64(fieldName).toString)
          case FLOAT32 => Some(struct.getFloat32(fieldName).toString)
          case FLOAT64 => Some(struct.getFloat64(fieldName).toString)
          case BOOLEAN => Some(struct.getBoolean(fieldName).toString)
          case STRING => Some(struct.getString(fieldName))
          case BYTES => Some(new String(struct.getBytes(fieldName)))
          case other => logger.error("Non-primitive values not supported: " + other);
            throw new IllegalArgumentException("Non-primitive values not supported: " + other)
        }
      }
  }

}
