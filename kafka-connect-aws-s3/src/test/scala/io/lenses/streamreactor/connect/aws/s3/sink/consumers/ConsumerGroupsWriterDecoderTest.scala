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
package io.lenses.streamreactor.connect.aws.s3.sink.consumers

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer

class ConsumerGroupsWriterDecoderTest extends AnyFunSuite with Matchers {
  test("return None if the version is greater 1") {
    val buffer = ByteBuffer.allocate(256)
    buffer.putShort(2)
    val record = new SinkRecord(
      "__consumer_offsets",
      77,
      Schema.BYTES_SCHEMA,
      buffer.array(),
      Schema.BYTES_SCHEMA,
      Array.emptyByteArray,
      -2,
    )
    ConsumerGroupsWriter.extractOffsets(record) shouldBe Right(None)
  }

  test("return the offset details") {
    val buffer = ByteBuffer.allocate(256)
    buffer.putShort(0.toShort)
    val group = "group"
    buffer.putShort(group.getBytes.length.toShort)
    buffer.put(group.getBytes)
    val topic = "topic"
    buffer.putShort(topic.getBytes.length.toShort)
    buffer.put(topic.getBytes)
    val partition = 11
    buffer.putInt(partition)

    val key         = buffer.array
    val valueBuffer = ByteBuffer.allocate(256)
    valueBuffer.putShort(0.toShort)
    val offset = 123L
    valueBuffer.putLong(offset)
    val metadata = "metadata"
    valueBuffer.putShort(metadata.getBytes.length.toShort)
    valueBuffer.put(metadata.getBytes)
    val commitTimestamp = 456L
    valueBuffer.putLong(commitTimestamp)

    val value = valueBuffer.array

    val record = new SinkRecord("__consumer_offsets", 77, Schema.BYTES_SCHEMA, key, Schema.BYTES_SCHEMA, value, 100)
    val actual = ConsumerGroupsWriter.extractOffsets(record)
    val expected = Right(
      Some(
        WriteOffset(
          OffsetDetails(
            OffsetKey(0.toShort, GroupTopicPartition(group, topic, partition)),
            OffsetAndMetadata(offset, -1, "metadata", commitTimestamp, -1L),
          ),
        ),
      ),
    )
    actual shouldBe expected
  }
  test("return None if the record key is null") {
    val record = new SinkRecord("__consumer_offsets",
                                77,
                                Schema.BYTES_SCHEMA,
                                null,
                                Schema.BYTES_SCHEMA,
                                Array.emptyByteArray,
                                100,
    )
    ConsumerGroupsWriter.extractOffsets(record) shouldBe Right(None)
  }
  test("return None if the record value is null") {
    val buffer = ByteBuffer.allocate(256)
    buffer.putShort(0.toShort)
    val group = "group"
    buffer.putShort(group.getBytes.length.toShort)
    buffer.put(group.getBytes)
    val topic = "topic"
    buffer.putShort(topic.getBytes.length.toShort)
    buffer.put(topic.getBytes)
    val partition = 11
    buffer.putInt(partition)

    val key    = buffer.array
    val record = new SinkRecord("__consumer_offsets", 77, Schema.BYTES_SCHEMA, key, Schema.BYTES_SCHEMA, null, 100)
    ConsumerGroupsWriter.extractOffsets(record) shouldBe
      Right(Some(DeleteOffset(GroupTopicPartition("group", "topic", 11))))
  }
  test("return an error when the record key is non bytes") {
    val record = new SinkRecord("__consumer_offsets",
                                77,
                                Schema.STRING_SCHEMA,
                                "key",
                                Schema.BYTES_SCHEMA,
                                Array.emptyByteArray,
                                100,
    )
    ConsumerGroupsWriter.extractOffsets(record) match {
      case Left(value) =>
        value shouldBe a[ConnectException]
        value.getMessage shouldBe "The record key is not a byte array. Make sure the connector configuration uses 'key.converter=org.apache.kafka.connect.converters.ByteArrayConverter'."
      case Right(_) => fail("Expecting an error but got a value instead.")
    }

  }
  test("return an error when the value is not a byte array") {
    val buffer = ByteBuffer.allocate(256)
    buffer.putShort(0.toShort)
    val group = "group"
    buffer.putShort(group.getBytes.length.toShort)
    buffer.put(group.getBytes)
    val topic = "topic"
    buffer.putShort(topic.getBytes.length.toShort)
    buffer.put(topic.getBytes)
    val partition = 11
    buffer.putInt(partition)

    val key    = buffer.array
    val record = new SinkRecord("__consumer_offsets", 77, Schema.BYTES_SCHEMA, key, Schema.STRING_SCHEMA, "value", 100)
    ConsumerGroupsWriter.extractOffsets(record) match {
      case Left(value) =>
        value shouldBe a[ConnectException]
        value.getMessage shouldBe "The record value is not a byte array. Make sure the connector configuration uses 'value.converter=org.apache.kafka.connect.converters.ByteArrayConverter'."
      case Right(_) => fail("Expecting an error but got a value instead.")
    }
  }
  test("returns the offset details when the version is 3") {
    val buffer = ByteBuffer.allocate(256)
    buffer.putShort(0.toShort)
    val group = "group"
    buffer.putShort(group.getBytes.length.toShort)
    buffer.put(group.getBytes)
    val topic = "topic"
    buffer.putShort(topic.getBytes.length.toShort)
    buffer.put(topic.getBytes)
    val partition = 11
    buffer.putInt(partition)

    val key         = buffer.array
    val valueBuffer = ByteBuffer.allocate(256)
    valueBuffer.putShort(3.toShort)
    val offset = 123L
    valueBuffer.putLong(offset)
    val leaderEpoch = 999
    valueBuffer.putInt(leaderEpoch)
    val metadata = "metadata"
    valueBuffer.putShort(metadata.getBytes.length.toShort)
    valueBuffer.put(metadata.getBytes)
    val commitTimestamp = 456L
    valueBuffer.putLong(commitTimestamp)

    val value = valueBuffer.array

    val record = new SinkRecord("__consumer_offsets", 77, Schema.BYTES_SCHEMA, key, Schema.BYTES_SCHEMA, value, -2)

    ConsumerGroupsWriter.extractOffsets(record) shouldBe Right(
      Some(
        WriteOffset(
          OffsetDetails(
            OffsetKey(0.toShort, GroupTopicPartition(group, topic, partition)),
            OffsetAndMetadata(offset, leaderEpoch, "metadata", commitTimestamp, -1L),
          ),
        ),
      ),
    )
  }
  test("return the offset details when the version is 1") {
    val buffer = ByteBuffer.allocate(256)
    buffer.putShort(0.toShort)
    val group = "group"
    buffer.putShort(group.getBytes.length.toShort)
    buffer.put(group.getBytes)
    val topic = "topic"
    buffer.putShort(topic.getBytes.length.toShort)
    buffer.put(topic.getBytes)
    val partition = 11
    buffer.putInt(partition)

    val key         = buffer.array
    val valueBuffer = ByteBuffer.allocate(256)
    valueBuffer.putShort(1.toShort)
    val offset = 123L
    valueBuffer.putLong(offset)
    val metadata = "metadata"
    valueBuffer.putShort(metadata.getBytes.length.toShort)
    valueBuffer.put(metadata.getBytes)
    val commitTimestamp = 456L
    valueBuffer.putLong(commitTimestamp)
    val expireTimestamp = 789L
    valueBuffer.putLong(expireTimestamp)

    val value = valueBuffer.array

    val record = new SinkRecord("__consumer_offsets", 77, Schema.BYTES_SCHEMA, key, Schema.BYTES_SCHEMA, value, -2)
    ConsumerGroupsWriter.extractOffsets(record) shouldBe Right(
      Some(
        WriteOffset(
          OffsetDetails(
            OffsetKey(0.toShort, GroupTopicPartition(group, topic, partition)),
            OffsetAndMetadata(offset, -1, "metadata", commitTimestamp, expireTimestamp),
          ),
        ),
      ),
    )
  }
}
