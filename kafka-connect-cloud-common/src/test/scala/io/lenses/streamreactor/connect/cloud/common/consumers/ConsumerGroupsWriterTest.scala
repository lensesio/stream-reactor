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

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference

class ConsumerGroupsWriterTest extends AnyFunSuite with Matchers {
  private val taskId = ConnectorTaskId("connectorA", 1, 1)
  test("write the offsets") {
    val location    = CloudObjectKey("bucket", None)
    val offsetArray = new AtomicReference[Array[Byte]](Array.emptyByteArray)
    val offsetKey   = new AtomicReference[String]("")
    val uploader = new Uploader {
      override def upload(source: ByteBuffer, bucket: String, path: String): Either[Throwable, Unit] = {
        offsetArray.set(source.array())
        offsetKey.set(path)
        Right(())
      }

      override def delete(bucket: String, path: String): Either[Throwable, Unit] = Right(())
      override def close(): Unit = {}
    }

    val group     = "lenses"
    val topic     = "topic"
    val partition = 11
    val offset    = 123L
    val records = List(
      new SinkRecord(
        "__consumer_offsets",
        77,
        null,
        generateOffsetKey(group, topic, partition),
        null,
        generateOffsetDetails(offset),
        100,
      ),
    )
    val writer = new ConsumerGroupsWriter(location, uploader, taskId)
    writer.write(records) shouldBe Right(())
    offsetArray.get() shouldBe ByteBuffer.allocate(8).putLong(offset).array()
    offsetKey.get() shouldBe s"$group/$topic/$partition"
  }

  test("writes the offsets for different groups and topics") {
    val location = CloudObjectKey("bucket", None)
    var writes   = Vector[(String, Array[Byte])]()

    val uploader = new Uploader {
      override def upload(source: ByteBuffer, bucket: String, path: String): Either[Throwable, Unit] = {
        writes = writes :+ (path, source.array())
        Right(())
      }
      override def delete(bucket: String, path: String): Either[Throwable, Unit] = Right(())
      override def close(): Unit = {}
    }
    val writer = new ConsumerGroupsWriter(location, uploader, taskId)
    val records = List(
      new SinkRecord(
        "__consumer_offsets",
        77,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(123L),
        100,
      ),
      new SinkRecord(
        "__consumer_offsets",
        77,
        null,
        generateOffsetKey("group1", "topic1", 2),
        null,
        generateOffsetDetails(456L),
        100,
      ),
      new SinkRecord(
        "__consumer_offsets",
        77,
        null,
        generateOffsetKey("group1", "topic2", 1),
        null,
        generateOffsetDetails(789L),
        100,
      ),
      new SinkRecord(
        "__consumer_offsets",
        77,
        null,
        generateOffsetKey("group2", "topic1", 1),
        null,
        generateOffsetDetails(101112L),
        100,
      ),
    )

    writer.write(records) shouldBe Right(())
    val writesToLong = writes.map(w => (w._1, ByteBuffer.wrap(w._2).getLong))
    writesToLong should contain theSameElementsAs List(
      ("group1/topic1/1", 123L),
      ("group1/topic1/2", 456L),
      ("group1/topic2/1", 789L),
      ("group2/topic1/1", 101112L),
    )
  }

  test("write once the entries for a group-topic-partition in one .write call") {
    val location = CloudObjectKey("bucket", None)
    var writes   = Vector[(String, Array[Byte])]()

    val uploader = new Uploader {
      override def upload(source: ByteBuffer, bucket: String, path: String): Either[Throwable, Unit] = {
        writes = writes :+ (path, source.array())
        Right(())
      }
      override def delete(bucket: String, path: String): Either[Throwable, Unit] = Right(())
      override def close(): Unit = {}
    }
    val writer = new ConsumerGroupsWriter(location, uploader, taskId)
    val records = List(
      new SinkRecord(
        "__consumer_offsets",
        77,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(123L),
        100,
      ),
      new SinkRecord(
        "__consumer_offsets",
        77,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(456L),
        101,
      ),
    )

    writer.write(records) shouldBe Right(())
    val writesToLong = writes.map(w => (w._1, ByteBuffer.wrap(w._2).getLong))
    writesToLong should contain theSameElementsAs List(
      ("group1/topic1/1", 456L),
    )
  }
  test("fail when the uploader fails") {
    val location = CloudObjectKey("bucket", None)
    val uploader = new Uploader {
      override def upload(source: ByteBuffer, bucket: String, path: String): Either[Throwable, Unit] =
        Left(new RuntimeException("Boom!"))
      override def delete(bucket: String, path: String): Either[Throwable, Unit] = Right(())
      override def close(): Unit = {}
    }
    val writer = new ConsumerGroupsWriter(location, uploader, taskId)
    val records = List(
      new SinkRecord(
        "__consumer_offsets",
        77,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(123L),
        100,
      ),
    )
    val result = writer.write(records)
    result.isLeft shouldBe true
    result.left.getOrElse(fail("should not fail")).getMessage shouldBe "Boom!"
  }

  test("write the offset and delete on empty value payload") {
    val location = CloudObjectKey("bucket", None)
    var writes   = Vector[(String, Array[Byte])]()

    var deletes = Vector[String]()
    val uploader = new Uploader {
      override def upload(source: ByteBuffer, bucket: String, path: String): Either[Throwable, Unit] = {
        writes = writes :+ (path, source.array())
        Right(())
      }
      override def delete(bucket: String, path: String): Either[Throwable, Unit] = {
        deletes = deletes :+ path
        Right(())
      }
      override def close(): Unit = {}
    }
    val writer = new ConsumerGroupsWriter(location, uploader, taskId)
    val records = List(
      new SinkRecord(
        "__consumer_offsets",
        77,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(123L),
        100,
      ),
      new SinkRecord(
        "__consumer_offsets",
        77,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        null,
        100,
      ),
    )

    writer.write(records) shouldBe Right(())
    writes shouldBe empty
    deletes should contain theSameElementsAs List(
      "group1/topic1/1",
    )
  }

  test("write, delete , write writes the offset") {
    val location = CloudObjectKey("bucket", None)
    var writes   = Vector[(String, Array[Byte])]()

    var deletes = Vector[String]()
    val uploader = new Uploader {
      override def upload(source: ByteBuffer, bucket: String, path: String): Either[Throwable, Unit] = {
        writes = writes :+ (path, source.array())
        Right(())
      }

      override def delete(bucket: String, path: String): Either[Throwable, Unit] = {
        deletes = deletes :+ path
        Right(())
      }

      override def close(): Unit = {}
    }
    val writer = new ConsumerGroupsWriter(location, uploader, taskId)
    val records = List(
      new SinkRecord(
        "__consumer_offsets",
        77,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(123L),
        100,
      ),
      new SinkRecord(
        "__consumer_offsets",
        77,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        null,
        100,
      ),
      new SinkRecord(
        "__consumer_offsets",
        77,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(333L),
        100,
      ),
    )

    writer.write(records) shouldBe Right(())
    val writesToLong = writes.map(w => (w._1, ByteBuffer.wrap(w._2).getLong))
    writesToLong should contain theSameElementsAs List(
      ("group1/topic1/1", 333L),
    )
    deletes shouldBe empty
  }
  private def generateOffsetKey(group: String, topic: String, partition: Int): Array[Byte] = {
    val buffer = ByteBuffer.allocate(256)
    buffer.putShort(0.toShort)

    buffer.putShort(group.getBytes.length.toShort)
    buffer.put(group.getBytes)

    buffer.putShort(topic.getBytes.length.toShort)
    buffer.put(topic.getBytes)
    buffer.putInt(partition)
    buffer.array

  }

  private def generateOffsetDetails(offset: Long) = {
    val valueBuffer = ByteBuffer.allocate(256)
    valueBuffer.putShort(0.toShort)
    valueBuffer.putLong(offset)
    val metadata = "metadata"
    valueBuffer.putShort(metadata.getBytes.length.toShort)
    valueBuffer.put(metadata.getBytes)
    val commitTimestamp = 456L
    valueBuffer.putLong(commitTimestamp)
    valueBuffer.array
  }
}
