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
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.storage.S3FileMetadata
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.config.AvroFormatSelection
import io.lenses.streamreactor.connect.cloud.common.config.DataStorageSettings
import io.lenses.streamreactor.connect.cloud.common.config.JsonFormatSelection
import io.lenses.streamreactor.connect.cloud.common.formats.writer.ArraySinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.SinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.StructSinkData
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.WriterManagerCreator
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Count
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.LocalStagingArea
import io.lenses.streamreactor.connect.cloud.common.sink.config.OffsetSeekerOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay.Values
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionSelection.defaultPartitionSelection
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.LeftPadPaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.NoOpPaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingService
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.sink.naming.CloudKeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.OffsetFileNamer
import io.lenses.streamreactor.connect.cloud.common.utils.ITSampleSchemaAndData.firstUsers
import io.lenses.streamreactor.connect.cloud.common.utils.ITSampleSchemaAndData.users
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData.UsersSchemaDecimal
import org.apache.kafka.connect.data.Struct
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.jdk.CollectionConverters._

class S3JsonWriterManagerTest extends AnyFlatSpec with Matchers with S3ProxyContainerTest {

  private val writerManagerCreator = new WriterManagerCreator[S3FileMetadata, S3SinkConfig]()

  private val compressionCodec = UNCOMPRESSED.toCodec()

  private val TopicName  = "myTopic"
  private val PathPrefix = "streamReactorBackups"
  private implicit val cloudLocationValidator: S3LocationValidator.type = S3LocationValidator

  "json sink" should "write single json record using offset key naming" in {

    val bucketAndPrefix = CloudLocation(BucketName, PathPrefix.some)
    val config = S3SinkConfig(
      S3ConnectionConfig(
        None,
        Some(s3Container.identity.identity),
        Some(s3Container.identity.credential),
        AuthMode.Credentials,
      ),
      bucketOptions = Seq(
        CloudSinkBucketOptions(
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
          localStagingArea = LocalStagingArea(localRoot),
          dataStorage      = DataStorageSettings.disabled,
        ), // JsonS3Format
      ),
      offsetSeekerOptions = OffsetSeekerOptions(5),
      compressionCodec,
      batchDelete = true,
    )

    val sink   = writerManagerCreator.from(config)
    val topic  = Topic(TopicName)
    val offset = Offset(1)
    sink.write(
      TopicPartitionOffset(topic, 1, offset),
      MessageDetail(
        NullSinkData(None),
        StructSinkData(users.head),
        Map.empty[String, SinkData],
        Some(Instant.ofEpochMilli(111L)),
        topic,
        1,
        offset,
      ),
    )
    sink.close()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/1_111_111.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
  }

  "json sink" should "write schemas to json" in {

    val bucketAndPrefix = CloudLocation(BucketName, PathPrefix.some)
    val config = S3SinkConfig(
      S3ConnectionConfig(
        None,
        Some(s3Container.identity.identity),
        Some(s3Container.identity.credential),
        AuthMode.Credentials,
      ),
      bucketOptions = Seq(
        CloudSinkBucketOptions(
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
          localStagingArea = LocalStagingArea(localRoot),
          dataStorage      = DataStorageSettings.disabled,
        ),
      ),
      offsetSeekerOptions = OffsetSeekerOptions(5),
      compressionCodec,
      batchDelete = true,
    )

    val sink = writerManagerCreator.from(config)
    firstUsers.zipWithIndex.foreach {
      case (struct: Struct, index: Int) =>
        val topic  = Topic(TopicName)
        val offset = Offset(index.toLong + 1)
        sink.write(
          TopicPartitionOffset(topic, 1, offset),
          MessageDetail(
            NullSinkData(None),
            StructSinkData(struct),
            Map.empty[String, SinkData],
            Some(Instant.ofEpochMilli((index + 1).toLong)),
            topic,
            0,
            offset,
          ),
        )
    }

    sink.close()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/3_1_3.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}{"name":"tom","title":null,"salary":395.44}""",
    )
  }

  "json sink" should "write bigdecimal to json" in {

    val bucketAndPrefix = CloudLocation(BucketName, PathPrefix.some)
    val config = S3SinkConfig(
      S3ConnectionConfig(
        None,
        Some(s3Container.identity.identity),
        Some(s3Container.identity.credential),
        AuthMode.Credentials,
      ),
      bucketOptions = Seq(
        CloudSinkBucketOptions(
          TopicName.some,
          bucketAndPrefix,
          commitPolicy    = CommitPolicy(Count(1)),
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
          localStagingArea = LocalStagingArea(localRoot),
          dataStorage      = DataStorageSettings.disabled,
        ),
      ),
      offsetSeekerOptions = OffsetSeekerOptions(5),
      compressionCodec,
      batchDelete = true,
    )

    val sink = writerManagerCreator.from(config)

    val topic  = Topic(TopicName)
    val offset = Offset(1L)
    val usersWithDecimal =
      new Struct(UsersSchemaDecimal)
        .put("name", "sam")
        .put("title", "mr")
        .put(
          "salary",
          BigDecimal("100.43").setScale(18).bigDecimal,
        )
    sink.write(
      TopicPartitionOffset(topic, 1, offset),
      MessageDetail(
        NullSinkData(None),
        StructSinkData(usersWithDecimal),
        Map.empty[String, SinkData],
        Some(Instant.ofEpochMilli(5555L)),
        topic,
        0,
        offset,
      ),
    )

    sink.close()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/1_5555_5555.json") should be(
      s"""{"name":"sam","title":"mr","salary":100.430000000000000000}""",
    )
  }

  "json sink" should "write single json record when the input is not best practice: Array of POJO" in {

    val bucketAndPrefix = CloudLocation(BucketName, PathPrefix.some)
    val config = S3SinkConfig(
      S3ConnectionConfig(
        None,
        Some(s3Container.identity.identity),
        Some(s3Container.identity.credential),
        AuthMode.Credentials,
      ),
      bucketOptions = Seq(
        CloudSinkBucketOptions(
          TopicName.some,
          bucketAndPrefix,
          commitPolicy    = CommitPolicy(Count(1)),
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
          localStagingArea = LocalStagingArea(localRoot),
          dataStorage      = DataStorageSettings.disabled,
        ),
      ),
      offsetSeekerOptions = OffsetSeekerOptions(5),
      compressionCodec,
      batchDelete = true,
    )

    val sink   = writerManagerCreator.from(config)
    val topic  = Topic(TopicName)
    val offset = Offset(1)
    val listOfPojo: java.util.List[Pojo] = List(
      new Pojo("sam", "mr", 100.43),
      new Pojo("laura", "ms", 429.06),
    ).asJava

    sink.write(
      TopicPartitionOffset(topic, 1, offset),
      MessageDetail(
        NullSinkData(None),
        ArraySinkData(listOfPojo, None),
        Map.empty[String, SinkData],
        Some(Instant.ofEpochMilli(1L)),
        topic,
        1,
        offset,
      ),
    )
    sink.close()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/1_1_1.json") should be(
      """[{"name":"sam","title":"mr","salary":100.43},{"name":"laura","title":"ms","salary":429.06}]""",
    )
  }
}

//create a class with the following fields
//.put("name", "sam").put("title", "mr").put("salary", 100.43),
class Pojo {
  private var name:   String = _
  private var title:  String = _
  private var salary: Double = _

  def this(name: String, title: String, salary: Double) = {
    this()
    this.name   = name
    this.title  = title
    this.salary = salary
  }

  def getName: String = name
  def setName(name: String): Unit = this.name = name
  def getTitle: String = title
  def setTitle(title: String): Unit = this.title = title
  def getSalary: Double = salary
  def setSalary(salary: Double): Unit = this.salary = salary
}
