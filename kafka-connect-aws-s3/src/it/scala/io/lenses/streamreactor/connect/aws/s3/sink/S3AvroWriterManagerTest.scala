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
import io.lenses.streamreactor.common.config.base.RetryConfig
import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.common.errors.ErrorPolicyEnum
import io.lenses.streamreactor.connect.aws.s3.config._
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.storage.S3FileMetadata
import io.lenses.streamreactor.connect.cloud.common.utils.ITSampleSchemaAndData.firstUsers
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.config.AvroFormatSelection
import io.lenses.streamreactor.connect.cloud.common.config.DataStorageSettings
import io.lenses.streamreactor.connect.cloud.common.formats.AvroFormatReader
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.DefaultSchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.WriterManagerCreator
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Count
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.LocalStagingArea
import io.lenses.streamreactor.connect.cloud.common.sink.config.IndexOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay.Values
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionSelection.defaultPartitionSelection
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.LeftPadPaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.NoOpPaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingService
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingStrategy
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.StructSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.naming.CloudKeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.FileNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.OffsetFileNamer
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData.UsersSchemaDecimal
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import java.time.Instant

class S3AvroWriterManagerTest extends AnyFlatSpec with Matchers with S3ProxyContainerTest {

  private val writerManagerCreator = new WriterManagerCreator[S3FileMetadata, S3SinkConfig]()

  private val compressionCodec = UNCOMPRESSED.toCodec()

  private val TopicName            = "myTopic"
  private val PathPrefix           = "streamReactorBackups"
  private val schemaChangeDetector = DefaultSchemaChangeDetector
  private val avroFormatReader     = new AvroFormatReader

  private implicit val cloudLocationValidator: CloudLocationValidator = S3LocationValidator
  private val bucketAndPrefix = CloudLocation(BucketName, PathPrefix.some)
  private def avroConfig(fileNamer: FileNamer) = S3SinkConfig(
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
        commitPolicy    = CommitPolicy(Count(2)),
        formatSelection = AvroFormatSelection,
        keyNamer = new CloudKeyNamer(
          AvroFormatSelection,
          defaultPartitionSelection(Values),
          fileNamer,
          new PaddingService(Map[String, PaddingStrategy](
            "partition" -> NoOpPaddingStrategy,
            "offset"    -> LeftPadPaddingStrategy(12, 0),
          )),
        ),
        localStagingArea = LocalStagingArea(localRoot),
        dataStorage      = DataStorageSettings.disabled,
      ),
    ),
    indexOptions                = IndexOptions(5, ".indexes").some,
    compressionCodec            = compressionCodec,
    batchDelete                 = true,
    errorPolicy                 = ErrorPolicy(ErrorPolicyEnum.THROW),
    connectorRetryConfig        = new RetryConfig(1, 1L, 1.0),
    logMetrics                  = false,
    schemaChangeDetector        = schemaChangeDetector,
    skipNullValues              = false,
    latestSchemaForWriteEnabled = false,
  )

  "avro sink" should "write 2 records to avro format in s3" in {
    val sink = writerManagerCreator.from(avroConfig(new OffsetFileNamer(
      identity[String],
      AvroFormatSelection.extension,
      None,
    )))._2
    firstUsers.zipWithIndex.foreach {
      case (struct: Struct, index: Int) =>
        val writeRes = sink.write(
          TopicPartitionOffset(Topic(TopicName), 1, Offset((index + 1).toLong)),
          MessageDetail(
            NullSinkData(None),
            StructSinkData(struct),
            Map.empty[String, SinkData],
            Some(Instant.ofEpochMilli(index.toLong + 101)),
            Topic(TopicName),
            1,
            Offset((index + 1).toLong),
          ),
        )
        writeRes.isRight should be(true)
    }

    sink.close()

    val keys = listBucketPath(BucketName, "streamReactorBackups/myTopic/1/")
    keys.size should be(1)

    val byteArray = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/2_101_102.avro")
    val genericRecords: List[GenericRecord] = avroFormatReader.read(byteArray)
    genericRecords.size should be(2)

    genericRecords(0).get("name").toString should be("sam")
    genericRecords(1).get("name").toString should be("laura")

  }

  "avro sink" should "write 2 records to avro format in s3 and add a suffix to the key generated" in {
    val sink = writerManagerCreator.from(avroConfig(new OffsetFileNamer(
      identity[String],
      AvroFormatSelection.extension,
      Some("-my-suffix"),
    )))._2
    firstUsers.zipWithIndex.foreach {
      case (struct: Struct, index: Int) =>
        val writeRes = sink.write(
          TopicPartitionOffset(Topic(TopicName), 1, Offset((index + 1).toLong)),
          MessageDetail(
            NullSinkData(None),
            StructSinkData(struct),
            Map.empty[String, SinkData],
            Some(Instant.ofEpochMilli(index.toLong + 101)),
            Topic(TopicName),
            1,
            Offset((index + 1).toLong),
          ),
        )
        writeRes.isRight should be(true)
    }

    sink.close()

    val keys = listBucketPath(BucketName, "streamReactorBackups/myTopic/1/")
    keys.size should be(1)

    val byteArray = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/2_101_102-my-suffix.avro")
    val genericRecords: List[GenericRecord] = avroFormatReader.read(byteArray)
    genericRecords.size should be(2)

    genericRecords(0).get("name").toString should be("sam")
    genericRecords(1).get("name").toString should be("laura")

  }

  "avro sink" should "write multiple files and keeping the earliest timestamp" in {
    val sink = writerManagerCreator.from(avroConfig(new OffsetFileNamer(
      identity[String],
      AvroFormatSelection.extension,
      None,
    )))._2
    firstUsers.zip(List(0 -> 100, 1 -> 99, 2 -> 101, 3 -> 102)).foreach {
      case (struct: Struct, (index: Int, timestamp: Int)) =>
        val writeRes = sink.write(
          TopicPartitionOffset(Topic(TopicName), 1, Offset((index + 1).toLong)),
          MessageDetail(
            NullSinkData(None),
            StructSinkData(struct),
            Map.empty[String, SinkData],
            Some(Instant.ofEpochMilli(timestamp.toLong)),
            Topic(TopicName),
            1,
            Offset((index + 1).toLong),
          ),
        )
        writeRes.isRight should be(true)
    }

    sink.close()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    val byteArray = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/2_99_100.avro")
    val genericRecords: List[GenericRecord] = avroFormatReader.read(byteArray)
    genericRecords.size should be(2)

    genericRecords(0).get("name").toString should be("sam")
    genericRecords(1).get("name").toString should be("laura")

  }

  "avro sink" should "write BigDecimal" in {
    val sink = writerManagerCreator.from(avroConfig(new OffsetFileNamer(
      identity[String],
      AvroFormatSelection.extension,
      None,
    )))._2
    val usersWithDecimal1 =
      new Struct(UsersSchemaDecimal)
        .put("name", "sam")
        .put("title", "mr")
        .put(
          "salary",
          BigDecimal("100.43").setScale(18).bigDecimal,
        )
    val writeRes1 = sink.write(
      TopicPartitionOffset(Topic(TopicName), 1, Offset(1L)),
      MessageDetail(
        NullSinkData(None),
        StructSinkData(usersWithDecimal1),
        Map.empty,
        Some(Instant.ofEpochMilli(10L)),
        Topic(TopicName),
        1,
        Offset(1L),
      ),
    )

    writeRes1.isRight should be(true)

    val usersWithDecimal2 =
      new Struct(UsersSchemaDecimal)
        .put("name", "maria")
        .put("title", "ms")
        .put(
          "salary",
          BigDecimal("100.43").setScale(18).bigDecimal,
        )

    val writeRes2 = sink.write(
      TopicPartitionOffset(Topic(TopicName), 1, Offset(2L)),
      MessageDetail(
        NullSinkData(None),
        StructSinkData(usersWithDecimal2),
        Map.empty,
        Some(Instant.ofEpochMilli(10L)),
        Topic(TopicName),
        1,
        Offset(2L),
      ),
    )
    writeRes2.isRight should be(true)
    sink.close()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    val byteArray = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/2_10_10.avro")
    val genericRecords: List[GenericRecord] = avroFormatReader.read(byteArray)
    genericRecords.size should be(2)

    genericRecords(0).get("name").toString should be("sam")
    val bb1 = genericRecords(0).get("salary").asInstanceOf[ByteBuffer]
    Decimal.toLogical(Decimal.builder(18).optional().build(), bb1.array()) should be(
      BigDecimal(100.43).setScale(18).bigDecimal,
    )

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
      new Struct(secondSchema).put("name", "coco").put("designation", null).put("salary", 395.44),
    )

    val sink = writerManagerCreator.from(avroConfig(new OffsetFileNamer(
      identity[String],
      AvroFormatSelection.extension,
      None,
    )))._2
    firstUsers.concat(usersWithNewSchema).zipWithIndex.foreach {
      case (user, index) =>
        sink.write(
          TopicPartitionOffset(Topic(TopicName), 1, Offset((index + 1).toLong)),
          MessageDetail(
            NullSinkData(None),
            StructSinkData(user),
            Map.empty[String, SinkData],
            Some(Instant.ofEpochMilli(index.toLong)),
            Topic(TopicName),
            1,
            Offset((index + 1).toLong),
          ),
        )
    }
    sink.close()

    val keys = listBucketPath(BucketName, "streamReactorBackups/myTopic/1/")
    keys.size should be(3)

    // records 1 and 2
    val genericRecords1: List[GenericRecord] = avroFormatReader.read(
      remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/2_0_1.avro"),
    )
    genericRecords1.size should be(2)
    genericRecords1(0).get("name").toString should be("sam")
    genericRecords1(1).get("name").toString should be("laura")

    // record 3 only - next schema is different so ending the file
    val genericRecords2: List[GenericRecord] = avroFormatReader.read(
      remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/3_2_2.avro"),
    )
    genericRecords2.size should be(1)
    genericRecords2(0).get("name").toString should be("tom")

    // record 3 only - next schema is different so ending the file
    val genericRecords3: List[GenericRecord] = avroFormatReader.read(
      remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/5_3_4.avro"),
    )
    genericRecords3.size should be(2)
    genericRecords3(0).get("name").toString should be("bobo")
    genericRecords3(1).get("name").toString should be("momo")

  }
}
