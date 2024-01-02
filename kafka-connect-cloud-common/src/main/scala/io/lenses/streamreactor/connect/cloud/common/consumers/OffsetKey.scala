/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.consumers

import java.nio.ByteBuffer

/**
  * Mimics the Kafka core OffsetKey class.
  */
case class OffsetKey(version: Short, key: GroupTopicPartition)

object OffsetKey {

  /**
    * Deserializes the OffsetKey from a byte array.
    *
    * @param version the version of the OffsetKey
    * @param buffer  the buffer to deserialize from
    * @return the deserialized OffsetKey
    */
  def from(version: Short, buffer: ByteBuffer): Either[Throwable, OffsetKey] = {
    def readStringField(fieldName: String): Either[Throwable, String] = {
      val length = buffer.getShort
      if (length < 0) Left(new RuntimeException(s"non-nullable field $fieldName was serialized as null"))
      else {
        val bytes = new Array[Byte](length.toInt)
        buffer.get(bytes)
        Right(new String(bytes))
      }
    }

    for {
      group    <- readStringField("group")
      topic    <- readStringField("topic")
      partition = buffer.getInt
    } yield OffsetKey(version, GroupTopicPartition(group, topic, partition))
  }
}
