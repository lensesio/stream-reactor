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
package com.datamountaineer.streamreactor.connect.converters.source

import com.datamountaineer.streamreactor.common.converters.MsgKey
import com.sksamuel.avro4s.AvroSchema
import com.sksamuel.avro4s.RecordFormat
import io.confluent.connect.avro.AvroData
import org.apache.avro.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.Collections

class JsonConverterWithSchemaEvolutionTest extends AnyWordSpec with Matchers {
  val topic       = "the_real_topic"
  val sourceTopic = "source_topic"
  val avroData    = new AvroData(4)

  "JsonConverter" should {
    "throw IllegalArgumentException if payload is null" in {
      intercept[ConnectException] {
        val converter = new JsonConverterWithSchemaEvolution
        val _         = converter.convert("topic", "somesource", "1000", null)
      }
    }

    "handle a simple json" in {
      val json      = JacksonJson.toJson(Car("LaFerrari", "Ferrari", 2015, 963, 0.0001))
      val converter = new JsonConverterWithSchemaEvolution
      val record    = converter.convert(topic, sourceTopic, "100", json.getBytes)
      record.keySchema() shouldBe MsgKey.schema
      record.key().asInstanceOf[Struct].getString("topic") shouldBe sourceTopic
      record.key().asInstanceOf[Struct].getString("id") shouldBe "100"

      val schema =
        new Schema.Parser().parse(
          AvroSchema[CarOptional].toString
            .replace("\"name\":\"CarOptional\"", s"""\"name\":\"$sourceTopic\"""")
            .replace(
              s""",\"namespace\":\"${getClass.getCanonicalName.dropRight(getClass.getSimpleName.length + 1)}\"""",
              "",
            ),
        )
      val format = RecordFormat[CarOptional]
      val carOptional =
        format.to(CarOptional(Option("LaFerrari"), Option("Ferrari"), Option(2015), Option(963), Option(0.0001)))

      record.valueSchema() shouldBe avroData.toConnectSchema(schema)

      record.value() shouldBe avroData.toConnectData(schema, carOptional).value()
      record.sourcePartition() shouldBe null
      record.sourceOffset() shouldBe Collections.singletonMap(
        JsonConverterWithSchemaEvolution.ConfigKey,
        avroData.fromConnectSchema(avroData.toConnectSchema(schema)).toString(),
      )
    }
  }
}

case class Car(name: String, manufacturer: String, model: Long, bhp: Long, price: Double)

case class CarOptional(
  name:         Option[String],
  manufacturer: Option[String],
  model:        Option[Long],
  bhp:          Option[Long],
  price:        Option[Double],
)
