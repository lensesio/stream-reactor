/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.conversion

import io.lenses.streamreactor.connect.cloud.common.formats.writer._
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.kafka.connect.data.Decimal

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
        "catA" -> struct,
        "catB" -> null,
      ).asJava,
      Some(mapSchema),
    )

  }

  "convert" should "convert a short" in {
    val short: java.lang.Short = 123.toShort
    val sinkData = ValueToSinkDataConverter.apply(short, Option.empty)

    sinkData match {
      case ShortSinkData(v, Some(schema)) =>
        v shouldBe short
        schema.`type`() shouldBe Schema.INT16_SCHEMA.`type`()
      case _ =>
    }
  }

  "convert" should "handle BigDecimal" in {
    val decimal  = BigDecimal("123.456")
    val sinkData = ValueToSinkDataConverter.apply(decimal, Option.empty)

    sinkData match {
      case DecimalSinkData(v, Some(schema)) =>
        schema.`type`() shouldBe Schema.BYTES_SCHEMA.`type`()
        schema.parameters().get(DecimalSinkData.PRECISION_FIELD) shouldBe "6"
        schema.parameters().get(Decimal.SCALE_FIELD) shouldBe "3"
        v shouldBe Decimal.fromLogical(schema, decimal.bigDecimal)
      case _ => fail("Expected DecimalSinkData")
    }
  }
  "convert" should "handle java.math.BigDecimal" in {
    val decimal  = new java.math.BigDecimal("123.456")
    val sinkData = ValueToSinkDataConverter.apply(decimal, Option.empty)

    sinkData match {
      case DecimalSinkData(v, Some(schema)) =>
        schema.`type`() shouldBe Schema.BYTES_SCHEMA.`type`()
        schema.parameters().get(DecimalSinkData.PRECISION_FIELD) shouldBe "6"
        schema.parameters().get(Decimal.SCALE_FIELD) shouldBe "3"
        v shouldBe Decimal.fromLogical(schema, decimal)
      case _ => fail("Expected DecimalSinkData")
    }
  }
}
