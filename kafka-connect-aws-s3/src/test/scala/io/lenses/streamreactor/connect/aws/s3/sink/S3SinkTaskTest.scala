
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

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.S3SinkConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.formats.{AvroFormatReader, ParquetFormatReader}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3ProxyContext.{Credential, Identity}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.{S3ProxyContext, S3TestConfig, S3TestPayloadReader}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.jclouds.blobstore.options.ListContainerOptions
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class S3SinkTaskTest extends AnyFlatSpec with Matchers with S3TestConfig with MockitoSugar {

  import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._

  val parquetFormatReader = new ParquetFormatReader()
  val avroFormatReader = new AvroFormatReader()

  val s3SinkTask = new S3SinkTask()
  val PrefixName = "streamReactorBackups"
  val TopicName = "mytopic"

  val DefaultProps = Map(
    AWS_REGION -> "eu-west-1",
    AWS_ACCESS_KEY -> Identity,
    AWS_SECRET_KEY -> Credential,
    AUTH_MODE -> AuthMode.Credentials.toString,
    CUSTOM_ENDPOINT -> S3ProxyContext.Uri,
    ENABLE_VIRTUAL_HOST_BUCKETS -> "true"
  )

  private val records = users.zipWithIndex.map { case (user, k) =>
    new SinkRecord(TopicName, 1, null, null, schema, user, k)
  }

  "S3SinkTask" should "flush on configured flush time intervals" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_INTERVAL = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)

    blobStoreContext.getBlobStore.list(BucketName,
      ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1")
    ).size() should be(0)

    Thread.sleep(1200) // wait for 1000 millisecond so the next call to put will cause a flush

    task.put(Seq().asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(1)

    readFileToString("streamReactorBackups/mytopic/1/2.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}{"name":"tom","title":null,"salary":395.44}""")

  }

  "S3SinkTask" should "flush for every record when configured flush count size of 1" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(3)

    readFileToString("streamReactorBackups/mytopic/1/0.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}""")
    readFileToString("streamReactorBackups/mytopic/1/1.json", blobStoreContext) should be("""{"name":"laura","title":"ms","salary":429.06}""")
    readFileToString("streamReactorBackups/mytopic/1/2.json", blobStoreContext) should be("""{"name":"tom","title":null,"salary":395.44}""")

  }

  /**
    * The file sizes of the 3 records above come out as 44,46,44.
    * We're going to set the threshold to 80 - so once we've written 2 records to a file then the
    * second file should only contain a single record.  This second file won't have been written yet.
    */
  "S3SinkTask" should "flush on configured file size" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_SIZE = 80"
        )
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(1)

    readFileToString("streamReactorBackups/mytopic/1/1.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}""")

  }

  /**
    * The difference in this test is that the sink is opened again, which will cause the offsets to be copied to the
    * context
    */
  "S3SinkTask" should "put existing offsets to the context" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 1"
        )
      ).asJava

    val sinkTaskContext = mock[SinkTaskContext]
    task.initialize(sinkTaskContext)
    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(3)

    readFileToString("streamReactorBackups/mytopic/1/0.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}""")
    readFileToString("streamReactorBackups/mytopic/1/1.json", blobStoreContext) should be("""{"name":"laura","title":"ms","salary":429.06}""")
    readFileToString("streamReactorBackups/mytopic/1/2.json", blobStoreContext) should be("""{"name":"tom","title":null,"salary":395.44}""")

    verify(sinkTaskContext).offset(new TopicPartition("mytopic", 1), 2)
  }

  "S3SinkTask" should "write to parquet format" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"""insert into $BucketName:$PrefixName select * from $TopicName STOREAS `PARQUET` WITH_FLUSH_COUNT = 1""")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(3)

    val bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/0.parquet", blobStoreContext)

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords(0), "sam", "mr", 100.43)

  }

  "S3SinkTask" should "write to avro format" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(3)

    val bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/0.avro", blobStoreContext)

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords(0), "sam", "mr", 100.43)

  }


  "S3SinkTask" should "error when trying to write AVRO to text format" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `TEXT` WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    assertThrows[IllegalStateException] {
      task.put(records.asJava)
    }

    task.stop()
  }

  "S3SinkTask" should "write to text format" in {

    val textRecords = List(
      new SinkRecord(TopicName, 1, null, null, null, "Sausages", 0),
      new SinkRecord(TopicName, 1, null, null, null, "Mash", 1),
      new SinkRecord(TopicName, 1, null, null, null, "Peas", 2),
      new SinkRecord(TopicName, 1, null, null, null, "Gravy", 3)
    )
    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `TEXT` WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(2)

    val file1Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/1.text", blobStoreContext)
    new String(file1Bytes) should be ("Sausages\nMash\n")

    val file2Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/3.text", blobStoreContext)
    new String(file2Bytes) should be ("Peas\nGravy\n")

  }

  "S3SinkTask" should "write to csv format" in {

    val task = new S3SinkTask()

    val extraRecord = new SinkRecord(TopicName, 1, null, null, schema,
      new Struct(schema).put("name", "bob").put("title", "mr").put("salary", 200.86), 3)

    val allRecords : List[SinkRecord] = records :+ extraRecord

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `CSV` WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(allRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(2)

    val file1Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/1.csv", blobStoreContext)

    val file1Reader = new StringReader(new String(file1Bytes))
    val file1CsvReader = new CSVReader(file1Reader)

    file1CsvReader.readNext() should be (Array("name", "title", "salary"))
    file1CsvReader.readNext() should be (Array("sam", "mr", "100.43"))
    file1CsvReader.readNext() should be (Array("laura", "ms", "429.06"))

    val file2Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/mytopic/1/3.csv", blobStoreContext)

    val file2Reader = new StringReader(new String(file2Bytes))
    val file2CsvReader = new CSVReader(file2Reader)

    file2CsvReader.readNext() should be (Array("name", "title", "salary"))
    file2CsvReader.readNext() should be (Array("tom", "", "395.44"))
    file2CsvReader.readNext() should be (Array("bob", "mr", "200.86"))
    file2CsvReader.readNext() should be (null)
  }
}