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

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.aws.s3.config._
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.aws.s3.sink.config.OffsetSeekerOptions
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.sink.config.SinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.utils.ITSampleSchemaAndData.firstUsers
import io.lenses.streamreactor.connect.cloud.common.utils.ITSampleSchemaAndData.users
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.config.AvroFormatSelection
import io.lenses.streamreactor.connect.cloud.common.config.DataStorageSettings
import io.lenses.streamreactor.connect.cloud.common.config.JsonFormatSelection
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.SinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.StructSinkData
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Count
import io.lenses.streamreactor.connect.cloud.common.sink.config.LocalStagingArea
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay.Values
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionSelection.defaultPartitionSelection
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.LeftPadPaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.NoOpPaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingService
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.sink.naming.OffsetFileNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.CloudKeyNamer
import org.apache.kafka.connect.data.Struct
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3JsonWriterManagerTest extends AnyFlatSpec with Matchers with S3ProxyContainerTest {

  private val compressionCodec = UNCOMPRESSED.toCodec()

  private val TopicName  = "myTopic"
  private val PathPrefix = "streamReactorBackups"
  private implicit val cloudLocationValidator: S3LocationValidator.type = S3LocationValidator

  "json sink" should "write single json record" in {

    val bucketAndPrefix = CloudLocation(BucketName, PathPrefix.some)
    val config = S3SinkConfig(
      S3Config(
        None,
        Some(s3Container.identity.identity),
        Some(s3Container.identity.credential),
        AuthMode.Credentials,
      ),
      bucketOptions = Seq(
        SinkBucketOptions(
          TopicName.some,
          bucketAndPrefix,
          commitPolicy    = CommitPolicy(Count(1)),
          formatSelection = JsonFormatSelection,
          keyNamer = new CloudKeyNamer(
            JsonFormatSelection,
            defaultPartitionSelection(Values),
            new OffsetFileNamer(
              identity[String],
              JsonFormatSelection.extension,
            ),
            new PaddingService(Map[String, PaddingStrategy](
              "partition" -> NoOpPaddingStrategy,
              "offset"    -> LeftPadPaddingStrategy(12, 0),
            )),
          ),
          localStagingArea   = LocalStagingArea(localRoot),
          partitionSelection = defaultPartitionSelection(Values),
          dataStorage        = DataStorageSettings.disabled,
        ), // JsonS3Format
      ),
      offsetSeekerOptions = OffsetSeekerOptions(5),
      compressionCodec,
      batchDelete = true,
    )

    val sink   = S3WriterManagerCreator.from(config)
    val topic  = Topic(TopicName)
    val offset = Offset(1)
    sink.write(
      TopicPartitionOffset(topic, 1, offset),
      MessageDetail(NullSinkData(None), StructSinkData(users.head), Map.empty[String, SinkData], None, topic, 1, offset),
    )
    sink.close()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/1.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
  }

  "json sink" should "write schemas to json" in {

    val bucketAndPrefix = CloudLocation(BucketName, PathPrefix.some)
    val config = S3SinkConfig(
      S3Config(
        None,
        Some(s3Container.identity.identity),
        Some(s3Container.identity.credential),
        AuthMode.Credentials,
      ),
      bucketOptions = Seq(
        SinkBucketOptions(
          TopicName.some,
          bucketAndPrefix,
          commitPolicy    = CommitPolicy(Count(3)),
          formatSelection = JsonFormatSelection,
          keyNamer = new CloudKeyNamer(
            AvroFormatSelection,
            defaultPartitionSelection(Values),
            new OffsetFileNamer(
              identity[String],
              JsonFormatSelection.extension,
            ),
            new PaddingService(Map[String, PaddingStrategy](
              "partition" -> NoOpPaddingStrategy,
              "offset"    -> LeftPadPaddingStrategy(12, 0),
            )),
          ),
          localStagingArea   = LocalStagingArea(localRoot),
          partitionSelection = defaultPartitionSelection(Values),
          dataStorage        = DataStorageSettings.disabled,
        ),
      ),
      offsetSeekerOptions = OffsetSeekerOptions(5),
      compressionCodec,
      batchDelete = true,
    )

    val sink = S3WriterManagerCreator.from(config)
    firstUsers.zipWithIndex.foreach {
      case (struct: Struct, index: Int) =>
        val topic  = Topic(TopicName)
        val offset = Offset(index.toLong + 1)
        sink.write(
          TopicPartitionOffset(topic, 1, offset),
          MessageDetail(NullSinkData(None), StructSinkData(struct), Map.empty[String, SinkData], None, topic, 0, offset),
        )
    }

    sink.close()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/3.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}{"name":"tom","title":null,"salary":395.44}""",
    )
  }

}
