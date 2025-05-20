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
package io.lenses.streamreactor.connect.redis.sink.rowkeys

import io.lenses.streamreactor.connect.redis.sink.rowkeys.StringSinkRecordKeyBuilder
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StringSinkRecordKeyBuilderTest extends AnyWordSpec with Matchers {
  val keyRowKeyBuilder = new StringSinkRecordKeyBuilder()

  "SinkRecordKeyStringKeyBuilder" should {

    "create the right key from the Schema key value - Byte" in {
      val b          = 123.toByte
      val sinkRecord = new SinkRecord("", 1, Schema.INT8_SCHEMA, b, Schema.FLOAT64_SCHEMA, Nil, 0)

      keyRowKeyBuilder.build(sinkRecord) shouldBe "123"

    }
    "create the right key from the Schema key value - String" in {
      val s          = "somekey"
      val sinkRecord = new SinkRecord("", 1, Schema.STRING_SCHEMA, s, Schema.FLOAT64_SCHEMA, Nil, 0)

      keyRowKeyBuilder.build(sinkRecord) shouldBe s
    }

    "create the right key from the Schema key value - Bytes" in {
      val bArray     = Array(23.toByte, 24.toByte, 242.toByte)
      val sinkRecord = new SinkRecord("", 1, Schema.BYTES_SCHEMA, bArray, Schema.FLOAT64_SCHEMA, Nil, 0)
      keyRowKeyBuilder.build(sinkRecord) shouldBe bArray.toString
    }
    "create the right key from the Schema key value - Boolean" in {
      val bool       = true
      val sinkRecord = new SinkRecord("", 1, Schema.BOOLEAN_SCHEMA, bool, Schema.FLOAT64_SCHEMA, Nil, 0)

      keyRowKeyBuilder.build(sinkRecord) shouldBe "true"

    }
  }
}
