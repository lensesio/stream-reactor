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

package io.lenses.streamreactor.connect.aws.s3.sink.conversion

import io.lenses.streamreactor.connect.aws.s3.model.MapSinkData
import io.lenses.streamreactor.connect.aws.s3.model.NullSinkData
import io.lenses.streamreactor.connect.aws.s3.model.StringSinkData
import io.lenses.streamreactor.connect.aws.s3.model.StructSinkData
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava

class ValueToSinkDataConverterTest extends AnyFlatSpec with Matchers {

  "convert" should "convert a map with a schema" in {

    val structSchema = SchemaBuilder.struct()
      .optional()
      .field("name", SchemaBuilder.string().required().build())
      .field("title", SchemaBuilder.string().optional().build())
      .field("salary", SchemaBuilder.float64().optional().build())
      .build()

    val struct = new Struct(structSchema).put("name", "laura").put("title", "ms").put("salary", 429.06)

    val map = Map(
      "catA" -> struct,
      "catB" -> null,
    ).asJava

    val mapSchema = SchemaBuilder.map(
      Schema.STRING_SCHEMA,
      structSchema,
    ).build()

    ValueToSinkDataConverter(map, Some(mapSchema)) shouldBe MapSinkData(
      Map(
        StringSinkData("catA") -> StructSinkData(struct),
        StringSinkData("catB") -> NullSinkData(),
      ),
      Some(mapSchema),
    )

  }

  "convert" should "convert a short" in {
    val short: java.lang.Short = 123.toShort
    val sinkData = ValueToSinkDataConverter.apply(short, Option.empty)

    sinkData should be(ShortSinkData(123))
  }
}
