/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.hbase

import com.datamountaineer.streamreactor.connect.hbase.BytesHelper._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{Matchers, WordSpec}

class GenericRowKeyBuilderTest extends WordSpec with Matchers {
  "GenericRowKeyBuilder" should {
    "use the topic, partition and offset to make the key" in {

      val topic = "sometopic"
      val partition = 2
      val offset = 1243L
      val sinkRecord = new SinkRecord(topic, partition, Schema.INT32_SCHEMA, 345, Schema.STRING_SCHEMA, "", offset)

      val keyBuilder = new GenericRowKeyBuilderBytes()
      val expected = Bytes.add(Array(topic.fromString(), keyBuilder.delimiterBytes, partition.fromString(),
        keyBuilder.delimiterBytes, offset.fromString()))
      keyBuilder.build(sinkRecord, Nil) shouldBe expected
    }
  }
}
