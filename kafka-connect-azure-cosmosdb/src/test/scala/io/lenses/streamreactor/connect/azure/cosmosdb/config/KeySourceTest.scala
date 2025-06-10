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
package io.lenses.streamreactor.connect.azure.cosmosdb.config

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.kafka.connect.sink.SinkRecord
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.mockito.MockitoSugar

class KeySourceTest extends AnyFunSuite with Matchers with MockitoSugar {

  test("KeyKeySource generates ID from SinkRecord key") {
    val sinkRecord = mock[SinkRecord]
    when(sinkRecord.key()).thenReturn("key-value")

    val result = KeyKeySource.generateId(Topic("topic").withPartition(0).atOffset(0), sinkRecord)
    result shouldEqual Right("key-value")
  }

  test("MetadataKeySource generates ID from topic, partition, and offset") {
    val topicPartitionOffset = Topic("topic").withPartition(1).atOffset(42)
    val sinkRecord           = mock[SinkRecord]

    val result = MetadataKeySource.generateId(topicPartitionOffset, sinkRecord)
    result shouldEqual Right("topic-1-42")
  }

  test("KeyPathKeySource generates ID from specified key path") {

    val structSchema = SchemaBuilder.struct()
      .optional()
      .field("keypath", SchemaBuilder.string().required().build())
      .build()

    val struct = new Struct(structSchema).put("keypath", "key-path-id")

    val sinkRecord = mock[SinkRecord]
    when(sinkRecord.key()).thenReturn(struct)
    when(sinkRecord.keySchema()).thenReturn(structSchema)

    val result = KeyPathKeySource("keypath").generateId(Topic("topic").withPartition(0).atOffset(0), sinkRecord)
    result shouldEqual Right("key-path-id")
  }

  test("ValuePathKeySource generates ID from specified value path") {

    val structSchema = SchemaBuilder.struct()
      .optional()
      .field("valuepath", SchemaBuilder.string().required().build())
      .build()

    val struct = new Struct(structSchema).put("valuepath", "value-path-id")

    val sinkRecord = mock[SinkRecord]
    when(sinkRecord.value()).thenReturn(struct)
    when(sinkRecord.valueSchema()).thenReturn(structSchema)

    val result = ValuePathKeySource("valuepath").generateId(Topic("topic").withPartition(0).atOffset(0), sinkRecord)
    result shouldEqual Right("value-path-id")
  }

}
