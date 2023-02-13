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
import io.lenses.streamreactor.connect.aws.s3.config.Format.Parquet
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.InitedConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.AwsClient
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.config.S3Config
import io.lenses.streamreactor.connect.aws.s3.formats.reader.ParquetFormatReader
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
import io.lenses.streamreactor.connect.aws.s3.utils.ITSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3ParquetWriterManagerTest extends AnyFlatSpec with Matchers with S3ProxyContainerTest {

  import helper._

  private val compressionCodec = UNCOMPRESSED.toCodec()
  private implicit val connectorTaskId: ConnectorTaskId = InitedConnectorTaskId("sinkName", 1, 1)

  private val TopicName           = "myTopic"
  private val PathPrefix          = "streamReactorBackups"
  private val parquetFormatReader = new ParquetFormatReader

  private val bucketAndPrefix = RemoteS3RootLocation(BucketName, Some(PathPrefix), allowSlash = false)
  private def parquetConfig = S3SinkConfig(
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
        commitPolicy       = DefaultCommitPolicy(None, None, Some(2)),
        fileNamingStrategy = new HierarchicalS3FileNamingStrategy(FormatSelection(Parquet), NoOpPaddingStrategy),
        formatSelection    = FormatSelection(Parquet),
        localStagingArea   = LocalStagingArea(localRoot),
      ),
    ),
    offsetSeekerOptions = OffsetSeekerOptions(5, true),
    compressionCodec,
  )

  "parquet sink" should "write 2 records to parquet format in s3" in {

    val sink = S3WriterManager.from(parquetConfig)
    firstUsers.zipWithIndex.foreach {
      case (struct: Struct, index: Int) =>
        sink.write(
          TopicPartitionOffset(Topic(TopicName), 1, Offset(index.toLong + 1)),
          MessageDetail(None, StructSinkData(struct), Map.empty[String, SinkData], None),
        )
    }

    sink.close()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    val byteArray = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/2.parquet")
    val genericRecords: List[GenericRecord] = parquetFormatReader.read(byteArray)
    genericRecords.size should be(2)

    genericRecords(0).get("name").toString should be("sam")
    genericRecords(1).get("name").toString should be("laura")

  }

  "parquet sink" should "write start a new file in case of schema change" in {

    val secondSchema: Schema = SchemaBuilder.struct()
      .field("name", SchemaBuilder.string().required().build())
      .field("designation", SchemaBuilder.string().optional().build())
      .field("salary", SchemaBuilder.float64().optional().build())
      .build()

    val usersWithNewSchema = List(
      new Struct(secondSchema).put("name", "bobo").put("designation", "mr").put("salary", 100.43),
      new Struct(secondSchema).put("name", "momo").put("designation", "ms").put("salary", 429.06),
      new Struct(secondSchema).put("name", "coco").put("designation", null).put("salary", 395.44),
    )

    val sink = S3WriterManager.from(parquetConfig)
    firstUsers.concat(usersWithNewSchema).zipWithIndex.foreach {
      case (user, index) =>
        sink.write(
          TopicPartitionOffset(Topic(TopicName), 1, Offset(index.toLong + 1)),
          MessageDetail(None, StructSinkData(user), Map.empty[String, SinkData], None),
        )
    }
    sink.close()

    //val list1 = listBucketPath(BucketName, "streamReactorBackups/myTopic/1/"))

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(3)

    // records 1 and 2
    val genericRecords1: List[GenericRecord] = parquetFormatReader.read(
      remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/2.parquet"),
    )
    genericRecords1.size should be(2)
    genericRecords1(0).get("name").toString should be("sam")
    genericRecords1(1).get("name").toString should be("laura")

    // record 3 only - next schema is different so ending the file
    val genericRecords2: List[GenericRecord] = parquetFormatReader.read(
      remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/3.parquet"),
    )
    genericRecords2.size should be(1)
    genericRecords2(0).get("name").toString should be("tom")

    // record 3 only - next schema is different so ending the file
    val genericRecords3: List[GenericRecord] = parquetFormatReader.read(
      remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/5.parquet"),
    )
    genericRecords3.size should be(2)
    genericRecords3(0).get("name").toString should be("bobo")
    genericRecords3(1).get("name").toString should be("momo")

  }
}
