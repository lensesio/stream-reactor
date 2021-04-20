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
import org.apache.kafka.connect.data.{Schema, Struct}

import java.util

/**
  * Extracts values from a Java map type
  */
object MapExtractor extends LazyLogging {

  def extractPathFromMap(map: util.Map[_, _], fieldName: PartitionNamePath, schema: Schema): Option[String] = {
    if (fieldName.hasTail) extractComplexType(map, fieldName, schema) else extractPrimitive(map, fieldName.head, schema)
  }

  private def extractComplexType(map: util.Map[_, _], fieldName: PartitionNamePath, schema: Schema): Option[String] = {
    val mapKey = fieldName.head
    Option(map.get(mapKey))
      .fold(Option.empty[String]) {
        case s: Struct => StructExtractor.extractPathFromStruct(s, fieldName.tail)
        case m: util.Map[Any, Any] => extractPathFromMap(m, fieldName.tail, schema.valueSchema())
        case other => logger.error("Unexpected type in Map Extractor: " + other)
          throw new IllegalArgumentException("Unexpected type in Map Extractor: " + other)
      }
  }

  private def extractPrimitive(map: util.Map[_, _], fieldName: String, mapSchema: Schema): Option[String] = {
    Option(mapSchema.valueSchema())
      .fold(Option.empty[String]) {
        valueSchema =>
          val value = map.get(fieldName)
          PrimitiveExtractor.extractPrimitiveValue(value, valueSchema)
      }
  }


}
