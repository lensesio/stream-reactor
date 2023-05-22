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
import io.lenses.streamreactor.connect.aws.s3.config.Format.Json
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.AwsClient
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.config.S3Config
import io.lenses.streamreactor.connect.aws.s3.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.aws.s3.formats.writer.SinkData
import io.lenses.streamreactor.connect.aws.s3.formats.writer.StructSinkData
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.sink.config.LocalStagingArea
import io.lenses.streamreactor.connect.aws.s3.sink.config.OffsetSeekerOptions
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.sink.config.SinkBucketOptions
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import org.apache.kafka.connect.data.Struct
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3JsonWriterManagerTest extends AnyFlatSpec with Matchers with S3ProxyContainerTest {

  import helper._
  import io.lenses.streamreactor.connect.aws.s3.utils.ITSampleSchemaAndData._

  private val compressionCodec = UNCOMPRESSED.toCodec()

  private val TopicName  = "myTopic"
  private val PathPrefix = "streamReactorBackups"
  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("sinkName", 1, 1)

  "json sink" should "write single json record" in {

    val bucketAndPrefix = RemoteS3RootLocation(BucketName, Some(PathPrefix), false)
    val config = S3SinkConfig(
      S3Config(
        None,
        Some(Identity),
        Some(Credential),
        AwsClient.Aws,
        AuthMode.Credentials,
      ),
      bucketOptions = Set(
        SinkBucketOptions(
          TopicName.some,
          bucketAndPrefix,
          commitPolicy       = DefaultCommitPolicy(None, None, Some(1)),
          formatSelection    = FormatSelection(Json),
          fileNamingStrategy = new HierarchicalS3FileNamingStrategy(FormatSelection(Json), NoOpPaddingStrategy),
          localStagingArea   = LocalStagingArea(localRoot),
        ), // JsonS3Format
      ),
      offsetSeekerOptions = OffsetSeekerOptions(5),
      compressionCodec,
    )

    val sink = S3WriterManager.from(config)
    sink.write(
      TopicPartitionOffset(Topic(TopicName), 1, Offset(1)),
      MessageDetail(None, StructSinkData(users.head), Map.empty[String, SinkData], None),
    )
    sink.close()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/1.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
  }

  "json sink" should "write schemas to json" in {

    val bucketAndPrefix = RemoteS3RootLocation(BucketName, Some(PathPrefix), false)
    val config = S3SinkConfig(
      S3Config(
        None,
        Some(Identity),
        Some(Credential),
        AwsClient.Aws,
        AuthMode.Credentials,
      ),
      bucketOptions = Set(
        SinkBucketOptions(
          TopicName.some,
          bucketAndPrefix,
          commitPolicy    = DefaultCommitPolicy(None, None, Some(3)),
          formatSelection = FormatSelection(Json),
          fileNamingStrategy =
            new HierarchicalS3FileNamingStrategy(FormatSelection(Json), NoOpPaddingStrategy), // JsonS3Format
          localStagingArea = LocalStagingArea(localRoot),
        ),
      ),
      offsetSeekerOptions = OffsetSeekerOptions(5),
      compressionCodec,
    )

    val sink = S3WriterManager.from(config)
    firstUsers.zipWithIndex.foreach {
      case (struct: Struct, index: Int) => sink.write(
          TopicPartitionOffset(Topic(TopicName), 1, Offset(index.toLong + 1)),
          MessageDetail(None, StructSinkData(struct), Map.empty[String, SinkData], None),
        )
    }

    sink.close()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/3.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}{"name":"tom","title":null,"salary":395.44}""",
    )
  }

}
