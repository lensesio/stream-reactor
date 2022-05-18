
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

import io.lenses.streamreactor.connect.aws.s3.config.Format.Avro
import io.lenses.streamreactor.connect.aws.s3.config.{AuthMode, AwsClient, FormatSelection, S3Config}
import io.lenses.streamreactor.connect.aws.s3.formats.AvroFormatReader
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.sink.config.{OffsetSeekerOptions, S3SinkConfig, SinkBucketOptions}
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.aws.s3.utils.ITSampleSchemaAndData._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3AvroWriterManagerTest extends AnyFlatSpec with Matchers with S3ProxyContainerTest {

  import helper._

  private val TopicName = "myTopic"
  private val PathPrefix = "streamReactorBackups"
  private val avroFormatReader = new AvroFormatReader

  private val bucketAndPrefix = RemoteS3RootLocation(BucketName, Some(PathPrefix), false)
  private def avroConfig = S3SinkConfig(S3Config(
    None,
    Some(Identity),
    Some(Credential),
    AwsClient.Aws,
    AuthMode.Credentials,
  ),
    bucketOptions = Set(
      SinkBucketOptions(
        TopicName,
        bucketAndPrefix,
        commitPolicy = DefaultCommitPolicy(None, None, Some(2)),
        formatSelection = FormatSelection(Avro),
        fileNamingStrategy = new HierarchicalS3FileNamingStrategy(FormatSelection(Avro)),
        localStagingArea = LocalStagingArea(localRoot),
      )
    ),
    offsetSeekerOptions = OffsetSeekerOptions(5, true)
  )

  "avro sink" should "write 2 records to avro format in s3" in {
    val sink = S3WriterManager.from(avroConfig, "sinkName")
    firstUsers.zipWithIndex.foreach {
      case (struct: Struct, index: Int) =>
        val writeRes = sink.write(
          TopicPartitionOffset(Topic(TopicName), 1, Offset((index + 1).toLong)), MessageDetail(None, StructSinkData(struct), Map.empty[String, SinkData], None))
        writeRes.isRight should be (true)
    }

    sink.close()

    listBucketPath( BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    val byteArray = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/2.avro")
    val genericRecords: List[GenericRecord] = avroFormatReader.read(byteArray)
    genericRecords.size should be(2)

    genericRecords(0).get("name").toString should be("sam")
    genericRecords(1).get("name").toString should be("laura")

  }

  "avro sink" should "write start a new file in case of schema change" in {

    val secondSchema: Schema = SchemaBuilder.struct()
      .field("name", SchemaBuilder.string().required().build())
      .field("designation", SchemaBuilder.string().optional().build())
      .field("salary", SchemaBuilder.float64().optional().build())
      .build()

    val usersWithNewSchema = List(
      new Struct(secondSchema).put("name", "bobo").put("designation", "mr").put("salary", 100.43),
      new Struct(secondSchema).put("name", "momo").put("designation", "ms").put("salary", 429.06),
      new Struct(secondSchema).put("name", "coco").put("designation", null).put("salary", 395.44)
    )

    val sink = S3WriterManager.from(avroConfig, "sinkName")
    firstUsers.concat(usersWithNewSchema).zipWithIndex.foreach {
      case (user, index) =>
        sink.write(TopicPartitionOffset(Topic(TopicName), 1, Offset((index + 1).toLong)), MessageDetail(None, StructSinkData(user), Map.empty[String, SinkData], None))
    }
    sink.close()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(3)

    // records 1 and 2
    val genericRecords1: List[GenericRecord] = avroFormatReader.read(
      remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/2.avro")
    )
    genericRecords1.size should be(2)
    genericRecords1(0).get("name").toString should be("sam")
    genericRecords1(1).get("name").toString should be("laura")

    // record 3 only - next schema is different so ending the file
    val genericRecords2: List[GenericRecord] = avroFormatReader.read(
      remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/3.avro")
    )
    genericRecords2.size should be(1)
    genericRecords2(0).get("name").toString should be("tom")

    // record 3 only - next schema is different so ending the file
    val genericRecords3: List[GenericRecord] = avroFormatReader.read(
      remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/5.avro")
    )
    genericRecords3.size should be(2)
    genericRecords3(0).get("name").toString should be("bobo")
    genericRecords3(1).get("name").toString should be("momo")

  }
}
