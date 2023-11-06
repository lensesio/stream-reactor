/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import java.util
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class S3ConsumerGroupsSinkTaskTest
    extends AnyFlatSpec
    with Matchers
    with S3ProxyContainerTest
    with MockitoSugar
    with LazyLogging {

  import helper._

  private val TopicName = "__consumer_offsets"

  private def DefaultProps: Map[String, String] = Map(
    AWS_ACCESS_KEY              -> Identity,
    AWS_SECRET_KEY              -> Credential,
    AUTH_MODE                   -> AuthMode.Credentials.toString,
    CUSTOM_ENDPOINT             -> uri(),
    ENABLE_VIRTUAL_HOST_BUCKETS -> "true",
    "name"                      -> "s3SinkTaskBuildLocalTest",
    AWS_REGION                  -> "eu-west-1",
    TASK_INDEX                  -> "1:1",
  )

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

  "S3ConsumerGroupsSinkTask" should "write the offsets" in {

    val task = new S3ConsumerGroupsSinkTask()

    val props = DefaultProps
      .combine(
        Map(
          S3_BUCKET_CONFIG -> BucketName,
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    val records: List[SinkRecord] = List(
      new SinkRecord(
        TopicName,
        1,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(123L),
        100,
      ),
      new SinkRecord(
        TopicName,
        1,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(456L),
        100,
      ),
      new SinkRecord(
        TopicName,
        1,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(789L),
        100,
      ),
    )
    task.put(records.asJava)

    {
      listBucketPath(BucketName, "group1/topic1").size should be(1)

      val byteArray = remoteFileAsBytes(BucketName, "group1/topic1/1")
      ByteBuffer.wrap(byteArray).getLong should be(789L)
    }

    val records2 = List(
      new SinkRecord(
        TopicName,
        1,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(999L),
        100,
      ),
    )

    task.put(records2.asJava)

    {
      listBucketPath(BucketName, "group1/topic1").size should be(1)

      val byteArray = remoteFileAsBytes(BucketName, "group1/topic1/1")
      ByteBuffer.wrap(byteArray).getLong should be(999L)
    }
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
  }

  "S3SinkTask" should "write the offsets with prefix" in {

    val task = new S3ConsumerGroupsSinkTask()

    val props = DefaultProps
      .combine(
        Map(
          S3_BUCKET_CONFIG -> (BucketName + ":a/b"),
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    val records: List[SinkRecord] = List(
      new SinkRecord(
        TopicName,
        1,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(123L),
        100,
      ),
      new SinkRecord(
        TopicName,
        1,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(456L),
        100,
      ),
      new SinkRecord(
        TopicName,
        1,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(789L),
        100,
      ),
    )
    task.put(records.asJava)

    listBucketPath(BucketName, "a/b/group1/topic1").size should be(1)

    val byteArray = remoteFileAsBytes(BucketName, "a/b/group1/topic1/1")
    ByteBuffer.wrap(byteArray).getLong should be(789L)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
  }

  "S3SinkTask" should "ignore records which are not offset keys" in {

    val task = new S3ConsumerGroupsSinkTask()

    val props = DefaultProps
      .combine(
        Map(
          S3_BUCKET_CONFIG -> BucketName,
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    val buffer = ByteBuffer.allocate(256)
    buffer.putShort(3)
    val records: List[SinkRecord] = List(
      new SinkRecord(
        TopicName,
        1,
        null,
        buffer.array(),
        null,
        "value".getBytes,
        100,
      ),
      new SinkRecord(
        TopicName,
        1,
        null,
        generateOffsetKey("group1", "topic1", 1),
        null,
        generateOffsetDetails(456L),
        100,
      ),
    )
    task.put(records.asJava)

    listBucketPath(BucketName, "group1/topic1").size should be(1)
    val array = remoteFileAsBytes(BucketName, "group1/topic1/1")
    ByteBuffer.wrap(array).getLong should be(456L)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
  }
  def createTask(context: SinkTaskContext, props: util.Map[String, String]): S3SinkTask = {
    reset(context)
    val task: S3SinkTask = new S3SinkTask()
    task.initialize(context)
    task.start(props)
    task
  }

}
