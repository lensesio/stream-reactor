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
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay.Values
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionSelection.defaultPartitionSelection
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.LeftPadPaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingService
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionPartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionSelection
import io.lenses.streamreactor.connect.cloud.common.sink.config.TopicPartitionField
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

  private val fileNamer: FileNamer =
    new OffsetFileNamer(paddingStrategy, JsonFormatSelection.extension)

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

  private val s3KeyNamer = CloudKeyNamer(formatSelection, partitionSelection, fileNamer, paddingService)

  test("stagingFile should generate the correct staging file path with no prefix") {
    val stagingDirectory = Files.createTempDirectory("myTempDir").toFile

    val result =
      s3KeyNamer.stagingFile(stagingDirectory, bucketNoPrefix, topicPartition.toTopicPartition, partitionValues)

    val fullPath     = result.value.getPath.replace(stagingDirectory.toString, "")
    val (path, uuid) = fullPath.splitAt(fullPath.length - 36)
    path shouldEqual s"/$TopicName/00$Partition/json/"
    UUID.fromString(uuid)
  }

  test("stagingFile should generate the correct staging file path") {
    val stagingDirectory = Files.createTempDirectory("myTempDir").toFile

    val result =
      s3KeyNamer.stagingFile(stagingDirectory, bucketAndPrefix, topicPartition.toTopicPartition, partitionValues)

    val fullPath     = result.value.getPath.replace(stagingDirectory.toString, "")
    val (path, uuid) = fullPath.splitAt(fullPath.length - 36)
    path shouldEqual s"/prefix/$TopicName/00$Partition/json/"
    UUID.fromString(uuid)
  }

  test("finalFilename should write to the root of the bucket with no prefix") {

    val result = s3KeyNamer.finalFilename(bucketNoPrefix, topicPartition, partitionValues)

    result.value.path.value shouldEqual s"$TopicName/00$Partition/0$Offset.json"
  }

  test("finalFilename should generate the correct final S3 location") {

    val result = s3KeyNamer.finalFilename(bucketAndPrefix, topicPartition, partitionValues)

    result.value.path.value shouldEqual s"prefix/$TopicName/00$Partition/0$Offset.json"
  }

}
