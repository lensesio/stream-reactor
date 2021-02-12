/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.common.converters.source

import java.util.Collections

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class JsonPassThroughConverterTest extends AnyWordSpec with Matchers {
  val topic = "the_real_topic"
  val sourceTopic = "source_topic"

  "JsonPassThroughConverter" should {
    "pass single message with no key through as json" in {
      val car = Car("LaFerrari", "Ferrari", 2015, 963, 0.0001)
      val json = JacksonJson.toJson(car)
      val converter = new JsonPassThroughConverter
      val record = converter.convert(topic, sourceTopic, "100", json.getBytes)
      record.keySchema() shouldBe null
      record.key() shouldBe "source_topic.100"

      record.valueSchema() shouldBe null

      record.value() shouldBe json
      record.sourcePartition() shouldBe Collections.singletonMap(Converter.TopicKey, sourceTopic)
      record.sourceOffset() shouldBe null
    }

    "pass single message with key through as json" in {
      val car = Car("LaFerrari", "Ferrari", 2015, 963, 0.0001)
      val json = JacksonJson.toJson(car)
      val converter = new JsonPassThroughConverter
      val keys = List("name", "manufacturer")
      val record = converter.convert(topic, sourceTopic, "100", json.getBytes, keys)
      record.keySchema() shouldBe null
      record.key() shouldBe "LaFerrari.Ferrari"

      record.valueSchema() shouldBe null

      record.value() shouldBe json
      record.sourcePartition() shouldBe Collections.singletonMap(Converter.TopicKey, sourceTopic)
      record.sourceOffset() shouldBe null
    }
  }
}
