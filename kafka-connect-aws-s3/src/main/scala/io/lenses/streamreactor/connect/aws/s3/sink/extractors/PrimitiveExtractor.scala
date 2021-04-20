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
import io.lenses.streamreactor.connect.aws.s3.sink.conversion.OptionConvert.convertToOption
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Schema.Type._

object PrimitiveExtractor extends LazyLogging {

  def extractPrimitiveValue(value: Any, schema : Schema): Option[String] = {
    schema.schema().`type`() match {
      case INT8 => convertToOption(value)
      case INT16 => convertToOption(value)
      case INT32 => convertToOption(value)
      case INT64 => convertToOption(value)
      case FLOAT32 => convertToOption(value)
      case FLOAT64 => convertToOption(value)
      case BOOLEAN => convertToOption(value)
      case STRING => convertToOption(value)
      case BYTES => Option(value).fold(Option.empty[String]) {
        case byteVal: Array[Byte] => Some(new String(byteVal))
      }
      case other => logger.error("Non-primitive values not supported: " + other)
        throw new IllegalArgumentException("Non-primitive values not supported: " + other)
    }
  }
}
