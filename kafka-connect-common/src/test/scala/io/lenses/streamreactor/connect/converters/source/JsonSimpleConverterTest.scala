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
package io.lenses.streamreactor.connect.converters.source

import io.lenses.streamreactor.common.converters.MsgKey
import org.apache.kafka.connect.data.Struct
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.Collections
import scala.jdk.CollectionConverters.ListHasAsScala

class JsonSimpleConverterTest extends AnyWordSpec with Matchers {
  val topic       = "the_real_topic"
  val sourceTopic = "source_topic"

  "JsonSimpleConverter" should {
    "convert from json to the struct" in {
      val car       = Car("LaFerrari", "Ferrari", 2015, 963, 0.0001)
      val json      = JacksonJson.toJson(car)
      val converter = new JsonSimpleConverter
      val record    = converter.convert(topic, sourceTopic, "100", json.getBytes)
      record.keySchema() shouldBe MsgKey.schema
      record.key() shouldBe MsgKey.getStruct(sourceTopic, "100")

      record.valueSchema().fields().asScala.count(f => f.name() == "name") shouldBe 1
      record.valueSchema().fields().asScala.count(f => f.name() == "manufacturer") shouldBe 1
      record.valueSchema().fields().asScala.count(f => f.name() == "model") shouldBe 1
      record.valueSchema().fields().asScala.count(f => f.name() == "bhp") shouldBe 1
      record.valueSchema().fields().asScala.count(f => f.name() == "price") shouldBe 1

      val struct = record.value().asInstanceOf[Struct]
      struct.get("name") shouldBe "LaFerrari"
      struct.get("manufacturer") shouldBe "Ferrari"
      struct.get("model") shouldBe 2015
      struct.get("bhp") shouldBe 963
      struct.get("price") shouldBe 0.0001

      record.sourcePartition() shouldBe Collections.singletonMap(Converter.TopicKey, sourceTopic)
      record.sourceOffset() shouldBe null
    }
  }
}
