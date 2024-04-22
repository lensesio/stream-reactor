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
package io.lenses.streamreactor.connect.cloud.common.sink.naming

import cats.implicits.none
import io.lenses.streamreactor.connect.cloud.common.config.FormatSelection
import io.lenses.streamreactor.connect.cloud.common.config.JsonFormatSelection
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.StringSinkData
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay.Values
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionSelection.defaultPartitionSelection
import io.lenses.streamreactor.connect.cloud.common.sink.config._
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.LeftPadPaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingService
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.mockito.ArgumentMatchers.anyString
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.util.UUID

class CloudKeyNamerTest extends AnyFunSuite with Matchers with OptionValues with EitherValues with MockitoSugar {
  implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator

  private val formatSelection:    FormatSelection    = JsonFormatSelection
  private val paddingStrategy:    PaddingStrategy    = LeftPadPaddingStrategy(3, '0')
  private val partitionSelection: PartitionSelection = defaultPartitionSelection(Values)

  private val bucketAndPrefix = CloudLocation("my-bucket", Some("prefix"))
  private val bucketNoPrefix  = CloudLocation("my-bucket", none)
  private val TopicName       = "my-topic"
  private val Partition       = 9
  private val Offset          = 81L

  private val topicPartition = Topic(TopicName).withPartition(Partition).atOffset(Offset)

  private val partitionValues = Map[PartitionField, String](
    TopicPartitionField     -> TopicName,
    PartitionPartitionField -> Partition.toString,
  )

  private val paddingService = mock[PaddingService]
  when(paddingService.padderFor(anyString)).thenReturn(paddingStrategy)

  test("the partition values do not replace / or \\ characters") {
    val partitionSelection =
      PartitionSelection(isCustom = false, List(HeaderPartitionField(PartitionNamePath("h"))), Values)

    val fileNamer: FileNamer =
      new OffsetFileNamerV0(paddingStrategy, JsonFormatSelection.extension)

    val keyNamer = CloudKeyNamer(formatSelection, partitionSelection, fileNamer, paddingService)
    val either: Either[SinkError, Map[PartitionField, String]] = keyNamer.processPartitionValues(
      MessageDetail(
        NullSinkData(None),
        NullSinkData(None),
        Map("h" -> StringSinkData("val1/val2")),
        None,
        topicPartition.topic,
        topicPartition.partition,
        topicPartition.offset,
      ),
      topicPartition.toTopicPartition,
    )

    either.value shouldBe Map(HeaderPartitionField(PartitionNamePath("h")) -> "val1/val2")
  }

  test("stagingFile should generate the correct staging file path with no prefix") {
    val stagingDirectory = Files.createTempDirectory("myTempDir").toFile

    val fileNamer: FileNamer =
      new OffsetFileNamerV0(paddingStrategy, JsonFormatSelection.extension)
    val s3KeyNamer = CloudKeyNamer(formatSelection, partitionSelection, fileNamer, paddingService)

    val result =
      s3KeyNamer.staging(stagingDirectory, bucketNoPrefix, topicPartition.toTopicPartition, partitionValues)

    val fullPath     = result.value.getPath.replace(stagingDirectory.toString, "")
    val (path, uuid) = fullPath.splitAt(fullPath.length - 36)
    path shouldEqual s"/$TopicName/00$Partition/json/"
    UUID.fromString(uuid)
  }

  test("should generate the correct staging file path") {
    val stagingDirectory = Files.createTempDirectory("myTempDir").toFile
    val fileNamer: FileNamer =
      new OffsetFileNamerV0(paddingStrategy, JsonFormatSelection.extension)
    val s3KeyNamer = CloudKeyNamer(formatSelection, partitionSelection, fileNamer, paddingService)

    val result =
      s3KeyNamer.staging(stagingDirectory, bucketAndPrefix, topicPartition.toTopicPartition, partitionValues)

    val fullPath     = result.value.getPath.replace(stagingDirectory.toString, "")
    val (path, uuid) = fullPath.splitAt(fullPath.length - 36)
    path shouldEqual s"/prefix/$TopicName/00$Partition/json/"
    UUID.fromString(uuid)
  }

  test("should write to the root of the bucket with no prefix") {
    val fileNamer: FileNamer =
      new OffsetFileNamerV0(paddingStrategy, JsonFormatSelection.extension)
    val s3KeyNamer = CloudKeyNamer(formatSelection, partitionSelection, fileNamer, paddingService)

    val result = s3KeyNamer.value(bucketNoPrefix, topicPartition, partitionValues, 0L)

    result.value.path.value shouldEqual s"$TopicName/00$Partition/0$Offset.json"
  }

  test("should generate the correct final S3 location for old format") {
    val fileNamer: FileNamer =
      new OffsetFileNamerV0(paddingStrategy, JsonFormatSelection.extension)
    val s3KeyNamer = CloudKeyNamer(formatSelection, partitionSelection, fileNamer, paddingService)

    val result = s3KeyNamer.value(bucketAndPrefix, topicPartition, partitionValues, 0L)

    result.value.path.value shouldEqual s"prefix/$TopicName/00$Partition/0$Offset.json"
  }

  test("should generate the correct final S3 location for v1 OffsetFileNamerV1 format") {
    val fileNamer: FileNamer =
      new OffsetFileNamerV1(paddingStrategy, JsonFormatSelection.extension)
    val s3KeyNamer = CloudKeyNamer(formatSelection, partitionSelection, fileNamer, paddingService)

    val result = s3KeyNamer.value(bucketAndPrefix, topicPartition, partitionValues, 101L)

    result.value.path.value shouldEqual s"prefix/$TopicName/00$Partition/0${Offset}_101.json"
  }

  test("should generate the correct final S3 location for TopicPartitionOffsetFileNamerV0 format") {
    val fileNamer: FileNamer =
      new TopicPartitionOffsetFileNamerV0(paddingStrategy, paddingStrategy, JsonFormatSelection.extension)
    val s3KeyNamer = CloudKeyNamer(formatSelection, partitionSelection, fileNamer, paddingService)

    val result = s3KeyNamer.value(bucketAndPrefix, topicPartition, partitionValues, 101L)

    result.value.path.value shouldEqual s"prefix/$TopicName/00$Partition/${topicPartition.topic.value}(009_0$Offset).json"
  }

  test("should generate the correct final S3 location for TopicPartitionOffsetFileNamerV1 format") {
    val fileNamer: FileNamer =
      new TopicPartitionOffsetFileNamerV1(paddingStrategy, paddingStrategy, JsonFormatSelection.extension)
    val s3KeyNamer = CloudKeyNamer(formatSelection, partitionSelection, fileNamer, paddingService)

    val result = s3KeyNamer.value(bucketAndPrefix, topicPartition, partitionValues, 101L)

    result.value.path.value shouldEqual s"prefix/$TopicName/00$Partition/${topicPartition.topic.value}(009_0${Offset}_101).json"
  }

}
