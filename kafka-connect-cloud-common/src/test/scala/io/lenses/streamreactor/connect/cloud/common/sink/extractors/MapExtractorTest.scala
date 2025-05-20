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

import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import org.apache.kafka.connect.data.SchemaBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava

class MapExtractorTest extends AnyFlatSpec with Matchers {

  private val stringSchema = SchemaBuilder.string().build()

  private val mapOfMapsOfStringsSchema = SchemaBuilder
    .map(stringSchema,
         SchemaBuilder.map(stringSchema, stringSchema)
           .build(),
    )

  private val mapOfMapsOfStrings = Map(
    "a" -> Map("b" -> "1").asJava,
    "c" -> Map("d" -> "2").asJava,
  ).asJava

  "lookupFieldValueFromStruct" should "handle map of maps" in {
    MapExtractor.extractPathFromMap(mapOfMapsOfStrings,
                                    PartitionNamePath("c", "d"),
                                    mapOfMapsOfStringsSchema,
    ) should be(Right("2"))
  }

  "lookupFieldValueFromStruct" should "handle map of maps without schema" in {
    MapExtractor.extractPathFromMap(mapOfMapsOfStrings, PartitionNamePath("c", "d"), null) should be(Right("2"))
  }
}
