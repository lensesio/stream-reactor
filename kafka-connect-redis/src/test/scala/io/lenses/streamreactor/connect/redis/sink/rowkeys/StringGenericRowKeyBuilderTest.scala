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

import io.lenses.streamreactor.connect.redis.sink.rowkeys.StringGenericRowKeyBuilder
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StringGenericRowKeyBuilderTest extends AnyWordSpec with Matchers {
  "StringGenericRowKeyBuilder" should {
    "use the topic, partition and offset to make the key" in {

      val topic      = "sometopic"
      val partition  = 2
      val offset     = 1243L
      val sinkRecord = new SinkRecord(topic, partition, Schema.INT32_SCHEMA, 345, Schema.STRING_SCHEMA, "", offset)

      val keyBuilder = new StringGenericRowKeyBuilder()
      val expected   = Seq(topic, partition.toString, offset.toString).mkString("|")
      keyBuilder.build(sinkRecord) shouldBe expected
    }
  }
}
