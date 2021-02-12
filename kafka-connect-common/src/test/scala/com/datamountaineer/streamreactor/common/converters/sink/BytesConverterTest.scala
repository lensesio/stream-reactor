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

package com.datamountaineer.streamreactor.common.converters.sink

import com.datamountaineer.streamreactor.common.converters.MsgKey
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BytesConverterTest extends AnyWordSpec with Matchers {
  private val converter = new BytesConverter()
  private val topic = "topicA"

  "Sink BytesConverter" should {
    "handle null payloads" in {
      val sinkRecord = converter.convert(topic, null)

      sinkRecord.keySchema() shouldBe null
      sinkRecord.key() shouldBe null
      sinkRecord.valueSchema() shouldBe Schema.BYTES_SCHEMA
      sinkRecord.value() shouldBe null
    }

    "handle non-null payloads" in {
      val expectedPayload: Array[Byte] = Array(245, 2, 10, 200, 22, 0, 0, 11).map(_.toByte)
      val data = new SinkRecord(topic, 0, null, "keyA", null, expectedPayload, 0)
      val sinkRecord = converter.convert(topic, data)

      sinkRecord.keySchema() shouldBe MsgKey.schema
      sinkRecord.key() shouldBe MsgKey.getStruct("topicA", "keyA")
      sinkRecord.valueSchema() shouldBe Schema.BYTES_SCHEMA
      sinkRecord.value() shouldBe expectedPayload
    }
  }
}
