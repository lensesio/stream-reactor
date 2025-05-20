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
package io.lenses.streamreactor.connect.converters.source

import io.lenses.streamreactor.common.converters.MsgKey
import org.apache.kafka.connect.data.Schema
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BytesConverterTest extends AnyWordSpec with Matchers {
  private val converter = new BytesConverter()
  private val topic     = "topicA"

  "BytesConverter" should {
    "handle null payloads" in {
      val sourceRecord = converter.convert(topic, "somesource", "100", null)

      sourceRecord.keySchema() shouldBe MsgKey.schema
      sourceRecord.key() shouldBe MsgKey.getStruct("somesource", "100")
      sourceRecord.valueSchema() shouldBe Schema.BYTES_SCHEMA
      sourceRecord.value() shouldBe null
    }

    "handle non-null payloads" in {
      val expectedPayload: Array[Byte] = Array(245, 2, 10, 200, 22, 0, 0, 11).map(_.toByte)
      val sourceRecord = converter.convert(topic, "somesource", "1001", expectedPayload)

      sourceRecord.keySchema() shouldBe MsgKey.schema
      sourceRecord.key() shouldBe MsgKey.getStruct("somesource", "1001")
      sourceRecord.valueSchema() shouldBe Schema.BYTES_SCHEMA
      sourceRecord.value() shouldBe expectedPayload
    }
  }
}
