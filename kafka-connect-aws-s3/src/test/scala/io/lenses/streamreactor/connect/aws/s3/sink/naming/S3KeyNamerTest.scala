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
package io.lenses.streamreactor.connect.aws.s3.sink.naming

import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.config.JsonFormatSelection
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.sink.config.PartitionField
import io.lenses.streamreactor.connect.aws.s3.sink.config.PartitionPartitionField
import io.lenses.streamreactor.connect.aws.s3.sink.config.PartitionSelection
import io.lenses.streamreactor.connect.aws.s3.sink.config.TopicPartitionField
import io.lenses.streamreactor.connect.aws.s3.sink.LeftPadPaddingStrategy
import io.lenses.streamreactor.connect.aws.s3.sink.PaddingStrategy
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.EitherValues
import org.scalatest.OptionValues

import java.nio.file.Files
import java.util.UUID

class S3KeyNamerTest extends AnyFunSuite with Matchers with OptionValues with EitherValues {

  private val formatSelection:    FormatSelection    = JsonFormatSelection
  private val paddingStrategy:    PaddingStrategy    = LeftPadPaddingStrategy(3, '0')
  private val partitionSelection: PartitionSelection = PartitionSelection.defaultPartitionSelection
  private val fileNamer:          S3FileNamer        = HierarchicalS3FileNamer
  private val bucketAndPrefix = S3Location("my-bucket", Some("prefix"))

  private val TopicName = "my-topic"
  private val Partition = 9
  private val Offset    = 81

  private val topicPartition = Topic(TopicName).withPartition(Partition).withOffset(Offset)

  private val partitionValues = Map[PartitionField, String](
    TopicPartitionField()     -> TopicName,
    PartitionPartitionField() -> Partition.toString,
  )
  private val s3KeyNamer = new S3KeyNamer(formatSelection, paddingStrategy.padString, partitionSelection, fileNamer)

  test("stagingFile should generate the correct staging file path") {
    val stagingDirectory = Files.createTempDirectory("myTempDir").toFile

    val result =
      s3KeyNamer.stagingFile(stagingDirectory, bucketAndPrefix, topicPartition.toTopicPartition, partitionValues)

    val fullPath     = result.value.getPath.replace(stagingDirectory.toString, "")
    val (path, uuid) = fullPath.splitAt(fullPath.length - 36)
    path shouldEqual s"/prefix/$TopicName/00$Partition/json/"
    UUID.fromString(uuid)
  }

  test("finalFilename should generate the correct final S3 location") {

    val result = s3KeyNamer.finalFilename(bucketAndPrefix, topicPartition, partitionValues)

    result.value.path.value shouldEqual s"prefix/$TopicName/00$Partition/0$Offset.json"
  }

}
