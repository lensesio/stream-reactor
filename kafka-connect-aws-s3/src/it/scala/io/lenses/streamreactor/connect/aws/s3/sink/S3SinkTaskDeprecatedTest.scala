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

import cats.implicits._
import com.opencsv.CSVReader
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.formats.AvroFormatReader
import io.lenses.streamreactor.connect.aws.s3.formats.reader.ParquetFormatReader
import io.lenses.streamreactor.connect.aws.s3.formats.writer.BytesFormatWriter
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.errors.RetriableException
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.header.Header
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.io.StringReader
import java.nio.file.Files
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.lang
import java.util
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Try

class S3SinkTaskDeprecatedTest extends AnyFlatSpec with Matchers with S3ProxyContainerTest with MockitoSugar with LazyLogging {

  import helper._
  import io.lenses.streamreactor.connect.aws.s3.utils.ITSampleSchemaAndData._

  private val parquetFormatReader = new ParquetFormatReader()
  private val avroFormatReader    = new AvroFormatReader()

  private val PrefixName = "streamReactorBackups"
  private val TopicName  = "myTopic"

  private def DeprecatedProps = Map(
    DEP_AWS_ACCESS_KEY              -> Identity,
    DEP_AWS_SECRET_KEY              -> Credential,
    DEP_AUTH_MODE                   -> AuthMode.Credentials.toString,
    DEP_CUSTOM_ENDPOINT             -> uri(),
    DEP_ENABLE_VIRTUAL_HOST_BUCKETS -> "true",
    "name"                          -> "s3SinkTaskBuildLocalTest",
    AWS_REGION                      -> "eu-west-1",
    TASK_INDEX                      -> "1:1",
  )

  private def DefaultProps = Map(
    AWS_ACCESS_KEY              -> Identity,
    AWS_SECRET_KEY              -> Credential,
    AUTH_MODE                   -> AuthMode.Credentials.toString,
    CUSTOM_ENDPOINT             -> uri(),
    ENABLE_VIRTUAL_HOST_BUCKETS -> "true",
    "name"                      -> "s3SinkTaskBuildLocalTest",
    AWS_REGION                  -> "eu-west-1",
    TASK_INDEX                  -> "1:1",
  )

  private val partitionedData: List[Struct] = List(
    new Struct(schema).put("name", "first").put("title", "primary").put("salary", null),
    new Struct(schema).put("name", "second").put("title", "secondary").put("salary", 100.00),
    new Struct(schema).put("name", "third").put("title", "primary").put("salary", 100.00),
    new Struct(schema).put("name", "first").put("title", null).put("salary", 200.00),
    new Struct(schema).put("name", "second").put("title", null).put("salary", 100.00),
    new Struct(schema).put("name", "third").put("title", null).put("salary", 100.00),
  )

  private val headerPartitionedRecords = partitionedData.zipWithIndex.map {
    case (user, k) =>
      new SinkRecord(TopicName,
                     1,
                     null,
                     null,
                     schema,
                     user,
                     k.toLong,
                     null,
                     null,
                     createHeaders(("headerPartitionKey", (k % 2).toString)),
      )
  }

  private val records = firstUsers.zipWithIndex.map {
    case (user, k) =>
      toSinkRecord(user, k)
  }

  private def toSinkRecord(user: Struct, k: Int, topicName: String = TopicName) =
    new SinkRecord(topicName, 1, null, null, schema, user, k.toLong)

  private val keySchema = SchemaBuilder.struct()
    .field("phonePrefix", SchemaBuilder.string().required().build())
    .field("region", SchemaBuilder.int32().optional().build())
    .build()

  private val keyPartitionedRecords = List(
    new SinkRecord(TopicName,
                   1,
                   null,
                   createKey(keySchema, ("phonePrefix", "+44"), ("region", 8)),
                   null,
                   users(0),
                   0,
                   null,
                   null,
    ),
    new SinkRecord(TopicName,
                   1,
                   null,
                   createKey(keySchema, ("phonePrefix", "+49"), ("region", 5)),
                   null,
                   users(1),
                   1,
                   null,
                   null,
    ),
    new SinkRecord(TopicName,
                   1,
                   null,
                   createKey(keySchema, ("phonePrefix", "+49"), ("region", 5)),
                   null,
                   users(2),
                   2,
                   null,
                   null,
    ),
  )

  "S3SinkTask" should "flush on configured flush time intervals" in {

    val task = new S3SinkTaskDeprecated()

    val props = DeprecatedProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_INTERVAL = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001").size should be(0)

    Thread.sleep(1200) // wait for 1000 millisecond so the next call to put will cause a flush

    task.put(Seq().asJava)
    task.put(records.asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000002.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}{"name":"tom","title":null,"salary":395.44}""",
    )

  }

  "S3SinkTask" should "throw error if prefix contains a slash" in {

    val task = new S3SinkTaskDeprecated()

    val prefixWithSlashes = "my/prefix/that/is/a/path"
    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$prefixWithSlashes select * from $TopicName WITH_FLUSH_INTERVAL = 1",
        ),
      ).asJava

    val intercepted = intercept[IllegalArgumentException] {
      task.start(props)
    }

    intercepted.getMessage should be("Nested prefix not currently supported")

  }

  "S3SinkTask" should "flush for every record when configured flush count size of 1" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 1"),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(3)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000000.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.json") should be(
      """{"name":"laura","title":"ms","salary":429.06}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000002.json") should be(
      """{"name":"tom","title":null,"salary":395.44}""",
    )

  }

  /**
    * The file sizes of the 3 records above come out as 44,46,44.
    * We're going to set the threshold to 80 - so once we've written 2 records to a file then the
    * second file should only contain a single record.  This second file won't have been written yet.
    */
  "S3SinkTask" should "flush on configured file size" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_SIZE = 80",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}""",
    )

  }

  "S3SinkTask" should "flush on configured file size for Parquet" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `Parquet` WITH_FLUSH_SIZE = 20",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(2)
    getMetadata(BucketName, "streamReactorBackups/myTopic/000000000001/000000000000.parquet").size should be >= 941L
    getMetadata(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.parquet").size should be >= 954L

    var genericRecords =
      parquetFormatReader.read(remoteFileAsBytes(BucketName,
                                                 "streamReactorBackups/myTopic/000000000001/000000000000.parquet",
      ))
    genericRecords.size should be(1)
    checkRecord(genericRecords.head, "sam", "mr", 100.43)

    genericRecords =
      parquetFormatReader.read(remoteFileAsBytes(BucketName,
                                                 "streamReactorBackups/myTopic/000000000001/000000000001.parquet",
      ))
    genericRecords.size should be(1)
    checkRecord(genericRecords.head, "laura", "ms", 429.06)

  }

  def createTask(context: SinkTaskContext, props: util.Map[String, String]): S3SinkTaskDeprecated = {
    reset(context)
    val task: S3SinkTaskDeprecated = new S3SinkTaskDeprecated()
    task.initialize(context)
    task.start(props)
    task
  }

  /**
    * The difference in this test is that the sink is opened again, which will cause the offsets to be copied to the
    * context
    */
  "S3SinkTask" should "put existing offsets to the context" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    val sinkTaskContext = mock[SinkTaskContext]
    task.initialize(sinkTaskContext)
    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(3)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000000.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.json") should be(
      """{"name":"laura","title":"ms","salary":429.06}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000002.json") should be(
      """{"name":"tom","title":null,"salary":395.44}""",
    )

    verify(sinkTaskContext).offset(new TopicPartition("myTopic", 1), 2)
  }

  "S3SinkTask" should "skip when kafka connect resends the same offsets after opening" in {

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `Parquet` WITH_FLUSH_COUNT = 2",
        ),
      ).asJava
    val context = mock[SinkTaskContext]

    var task: S3SinkTaskDeprecated = createTask(context, props)

    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    verify(context, never).offset(new TopicPartition("myTopic", 1), 0)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition("myTopic", 1)).asJava)
    task.stop()

    val list = listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/")
    list.size should be(1)
    list should contain("streamReactorBackups/myTopic/000000000001/000000000001.parquet")

    val modificationDate =
      getMetadata(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.parquet").lastModified

    task = createTask(context, props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    verify(context).offset(new TopicPartition("myTopic", 1), 1)
    task.put(records.asJava)

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(1)

    // file should not have been overwritten
    getMetadata(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.parquet").lastModified should be(
      modificationDate,
    )

    task.close(Seq(new TopicPartition("myTopic", 1)).asJava)
    task.stop()

    task = createTask(context, props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    verify(context).offset(new TopicPartition("myTopic", 1), 1)
    // only 1 "real" record so should leave it hanging again
    task.put(List(records(1), records(2)).asJava)

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(1)

    // file should not have been overwritten
    getMetadata(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.parquet").lastModified should be(
      modificationDate,
    )

    task.close(Seq(new TopicPartition("myTopic", 1)).asJava)
    task.stop()

    task = createTask(context, props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    verify(context).offset(new TopicPartition("myTopic", 1), 1)

    // this time we have an unseen record (3), which should be processed alongside (2) to give us a new file
    task.put(List(
      records(1),
      records(2),
      toSinkRecord(users(3), 3),
    ).asJava)

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(2)

    // file should not have been overwritten
    getMetadata(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.parquet").lastModified should be(
      modificationDate,
    )

    task.close(Seq(new TopicPartition("myTopic", 1)).asJava)
    task.stop()

  }

  "S3SinkTask" should "write to parquet format" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"""insert into $BucketName:$PrefixName select * from $TopicName STOREAS `PARQUET` WITH_FLUSH_COUNT = 1""",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(3)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000000.parquet")

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords.head, "sam", "mr", 100.43)

  }

  "S3SinkTask" should "write to avro format" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(3)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000000.avro")

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords.head, "sam", "mr", 100.43)

  }

  "S3SinkTask" should "error when trying to write AVRO to text format" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `TEXT` WITH_FLUSH_COUNT = 2",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    assertThrows[ConnectException] {
      task.put(records.asJava)
    }

    task.stop()
  }

  "S3SinkTask" should "write to text format" in {

    val textRecords = List(
      new SinkRecord(TopicName, 1, null, null, null, "Sausages", 0),
      new SinkRecord(TopicName, 1, null, null, null, "Mash", 1),
      new SinkRecord(TopicName, 1, null, null, null, "Peas", 2),
      new SinkRecord(TopicName, 1, null, null, null, "Gravy", 3),
    )
    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `TEXT` WITH_FLUSH_COUNT = 2",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(2)

    val file1Bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.text")
    new String(file1Bytes) should be("Sausages\nMash\n")

    val file2Bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000003.text")
    new String(file2Bytes) should be("Peas\nGravy\n")

  }

  "S3SinkTask" should "write to csv format with headers" in {

    val task = new S3SinkTaskDeprecated()

    val extraRecord = toSinkRecord(new Struct(schema).put("name", "bob").put("title", "mr").put("salary", 200.86), 3)

    val allRecords: List[SinkRecord] = records :+ extraRecord

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `CSV_WITHHEADERS` WITH_FLUSH_COUNT = 2",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(allRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(2)

    val file1Bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.csv")

    val file1Reader    = new StringReader(new String(file1Bytes))
    val file1CsvReader = new CSVReader(file1Reader)

    file1CsvReader.readNext() should be(Array("name", "title", "salary"))
    file1CsvReader.readNext() should be(Array("sam", "mr", "100.43"))
    file1CsvReader.readNext() should be(Array("laura", "ms", "429.06"))
    file1CsvReader.readNext() should be(null)

    val file2Bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000003.csv")

    val file2Reader    = new StringReader(new String(file2Bytes))
    val file2CsvReader = new CSVReader(file2Reader)

    file2CsvReader.readNext() should be(Array("name", "title", "salary"))
    file2CsvReader.readNext() should be(Array("tom", "", "395.44"))
    file2CsvReader.readNext() should be(Array("bob", "mr", "200.86"))
    file2CsvReader.readNext() should be(null)
  }

  "S3SinkTask" should "write to csv format without headers" in {

    val task = new S3SinkTaskDeprecated()

    val extraRecord = toSinkRecord(new Struct(schema).put("name", "bob").put("title", "mr").put("salary", 200.86), 3)

    val allRecords: List[SinkRecord] = records :+ extraRecord

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `CSV` WITH_FLUSH_COUNT = 2",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(allRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(2)

    val file1Bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.csv")

    val file1Reader    = new StringReader(new String(file1Bytes))
    val file1CsvReader = new CSVReader(file1Reader)

    file1CsvReader.readNext() should be(Array("sam", "mr", "100.43"))
    file1CsvReader.readNext() should be(Array("laura", "ms", "429.06"))
    file1CsvReader.readNext() should be(null)

    val file2Bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000003.csv")

    val file2Reader    = new StringReader(new String(file2Bytes))
    val file2CsvReader = new CSVReader(file2Reader)

    file2CsvReader.readNext() should be(Array("tom", "", "395.44"))
    file2CsvReader.readNext() should be(Array("bob", "mr", "200.86"))
    file2CsvReader.readNext() should be(null)
  }

  "S3SinkTask" should "use custom partitioning scheme and flush for every record" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name,title,salary WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(headerPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/").size should be(6)

    remoteFileAsString(
      BucketName,
      "streamReactorBackups/name=first/title=primary/salary=[missing]/myTopic(000000000001_000000000000).json",
    ) should be("""{"name":"first","title":"primary","salary":null}""")
    remoteFileAsString(
      BucketName,
      "streamReactorBackups/name=second/title=secondary/salary=100.0/myTopic(000000000001_000000000001).json",
    ) should be("""{"name":"second","title":"secondary","salary":100.0}""")
    remoteFileAsString(
      BucketName,
      "streamReactorBackups/name=third/title=primary/salary=100.0/myTopic(000000000001_000000000002).json",
    ) should be("""{"name":"third","title":"primary","salary":100.0}""")
    remoteFileAsString(
      BucketName,
      "streamReactorBackups/name=first/title=[missing]/salary=200.0/myTopic(000000000001_000000000003).json",
    ) should be("""{"name":"first","title":null,"salary":200.0}""")
    remoteFileAsString(
      BucketName,
      "streamReactorBackups/name=second/title=[missing]/salary=100.0/myTopic(000000000001_000000000004).json",
    ) should be("""{"name":"second","title":null,"salary":100.0}""")
    remoteFileAsString(
      BucketName,
      "streamReactorBackups/name=third/title=[missing]/salary=100.0/myTopic(000000000001_000000000005).json",
    ) should be("""{"name":"third","title":null,"salary":100.0}""")

  }

  /**
    * As soon as one file is eligible for writing, it will write all those from the same topic partition.  Therefore 4
    * files are written instead of 2, as there are 2 points at which the write is triggered and the half-full files must
    * be written as well as those reaching the threshold.
    */
  "S3SinkTask" should "use custom partitioning scheme and flush after two written records" in {

    val task = new S3SinkTaskDeprecated()

    val partitionedData: List[Struct] = List(
      new Struct(schema).put("name", "first").put("title", "primary").put("salary", null),
      new Struct(schema).put("name", "first").put("title", "secondary").put("salary", 100.00),
      new Struct(schema).put("name", "first").put("title", "primary").put("salary", 100.00),
      new Struct(schema).put("name", "second").put("title", "secondary").put("salary", 200.00),
      new Struct(schema).put("name", "second").put("title", "primary").put("salary", 100.00),
      new Struct(schema).put("name", "second").put("title", "secondary").put("salary", 100.00),
    )

    val records = partitionedData.zipWithIndex.map {
      case (user, k) =>
        toSinkRecord(user, k)
    }

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name,title WITH_FLUSH_COUNT = 2",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/").size should be(4)

    remoteFileAsString(BucketName,
                       "streamReactorBackups/name=first/title=primary/myTopic(000000000001_000000000002).json",
    ) should be(
      """{"name":"first","title":"primary","salary":null}{"name":"first","title":"primary","salary":100.0}""",
    )
    remoteFileAsString(BucketName,
                       "streamReactorBackups/name=first/title=secondary/myTopic(000000000001_000000000001).json",
    ) should be(
      """{"name":"first","title":"secondary","salary":100.0}""",
    )
    remoteFileAsString(BucketName,
                       "streamReactorBackups/name=second/title=secondary/myTopic(000000000001_000000000005).json",
    ) should be(
      """{"name":"second","title":"secondary","salary":200.0}{"name":"second","title":"secondary","salary":100.0}""",
    )
    remoteFileAsString(BucketName,
                       "streamReactorBackups/name=second/title=primary/myTopic(000000000001_000000000004).json",
    ) should be(
      """{"name":"second","title":"primary","salary":100.0}""",
    )

  }

  "S3SinkTask" should "use custom partitioning with value display only" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name,title,salary WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(headerPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/").size should be(6)

    remoteFileAsString(BucketName,
                       "streamReactorBackups/first/primary/[missing]/myTopic(000000000001_000000000000).json",
    ) should be(
      """{"name":"first","title":"primary","salary":null}""",
    )
    remoteFileAsString(BucketName,
                       "streamReactorBackups/second/secondary/100.0/myTopic(000000000001_000000000001).json",
    ) should be(
      """{"name":"second","title":"secondary","salary":100.0}""",
    )
    remoteFileAsString(BucketName,
                       "streamReactorBackups/third/primary/100.0/myTopic(000000000001_000000000002).json",
    ) should be(
      """{"name":"third","title":"primary","salary":100.0}""",
    )
    remoteFileAsString(BucketName,
                       "streamReactorBackups/first/[missing]/200.0/myTopic(000000000001_000000000003).json",
    ) should be(
      """{"name":"first","title":null,"salary":200.0}""",
    )
    remoteFileAsString(BucketName,
                       "streamReactorBackups/second/[missing]/100.0/myTopic(000000000001_000000000004).json",
    ) should be(
      """{"name":"second","title":null,"salary":100.0}""",
    )
    remoteFileAsString(BucketName,
                       "streamReactorBackups/third/[missing]/100.0/myTopic(000000000001_000000000005).json",
    ) should be(
      """{"name":"third","title":null,"salary":100.0}""",
    )

  }

  "S3SinkTask" should "write to bytes format and combine files" in {

    val stream = classOf[BytesFormatWriter].getResourceAsStream("/streamreactor-logo.png")
    val bytes: Array[Byte] = IOUtils.toByteArray(stream)
    val (bytes1, bytes2) = bytes.splitAt(bytes.length / 2)

    val textRecords = List(
      new SinkRecord(TopicName, 1, null, null, null, bytes1, 0),
      new SinkRecord(TopicName, 1, null, null, null, bytes2, 1),
    )
    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `BYTES_ValueOnly` WITH_FLUSH_COUNT = 2",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(1)

    val file1Bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.bytes")
    file1Bytes should be(bytes)

  }

  "S3SinkTask" should "support header values for data partitioning" in {

    val textRecords = List(
      new SinkRecord(TopicName,
                     1,
                     null,
                     null,
                     null,
                     users(0),
                     0,
                     null,
                     null,
                     createHeaders(("phonePrefix", "+44"), ("region", "8")),
      ),
      new SinkRecord(TopicName,
                     1,
                     null,
                     null,
                     null,
                     users(1),
                     1,
                     null,
                     null,
                     createHeaders(("phonePrefix", "+49"), ("region", "5")),
      ),
      new SinkRecord(TopicName,
                     1,
                     null,
                     null,
                     null,
                     users(2),
                     2,
                     null,
                     null,
                     createHeaders(("phonePrefix", "+49"), ("region", "5")),
      ),
    )
    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.phonePrefix,_header.region STOREAS `CSV` WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/").size should be(3)

    val file1CsvReader: CSVReader =
      openCsvReaderToBucketFile("streamReactorBackups/phonePrefix=+44/region=8/myTopic(000000000001_000000000000).csv")
    file1CsvReader.readNext() should be(Array("sam", "mr", "100.43"))
    file1CsvReader.readNext() should be(null)

    val file2CsvReader: CSVReader =
      openCsvReaderToBucketFile("streamReactorBackups/phonePrefix=+49/region=5/myTopic(000000000001_000000000001).csv")
    file2CsvReader.readNext() should be(Array("laura", "ms", "429.06"))
    file2CsvReader.readNext() should be(null)

    val file3CsvReader: CSVReader =
      openCsvReaderToBucketFile("streamReactorBackups/phonePrefix=+49/region=5/myTopic(000000000001_000000000002).csv")
    file3CsvReader.readNext() should be(Array("tom", "", "395.44"))
    file3CsvReader.readNext() should be(null)
  }

  "S3SinkTask" should "combine header and value-extracted partition" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.headerPartitionKey,_value.name STOREAS `CSV` WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(headerPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(6)

    fileList should contain allOf (
      "streamReactorBackups/headerPartitionKey=0/name=first/myTopic(000000000001_000000000000).csv",
      "streamReactorBackups/headerPartitionKey=1/name=second/myTopic(000000000001_000000000001).csv",
      "streamReactorBackups/headerPartitionKey=0/name=third/myTopic(000000000001_000000000002).csv",
      "streamReactorBackups/headerPartitionKey=1/name=first/myTopic(000000000001_000000000003).csv",
      "streamReactorBackups/headerPartitionKey=0/name=second/myTopic(000000000001_000000000004).csv",
      "streamReactorBackups/headerPartitionKey=1/name=third/myTopic(000000000001_000000000005).csv"
    )
  }

  "S3SinkTask" should "fail when header value is missing from the record when header partitioning activated" in {

    val textRecords = List(
      createSinkRecord(1, users(0), 0, createHeaders(("feet", "5"))),
      createSinkRecord(1, users(1), 0, createHeaders(("hair", "blue"), ("feet", "5"))),
    )

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.hair,_header.feet STOREAS `CSV` WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val caught = intercept[ConnectException] {
      task.put(textRecords.asJava)
    }

    assert(caught.getMessage.endsWith("Header 'hair' not found in message"))

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/").size should be(0)

  }

  "S3SinkTask" should "support numeric header data types" in {

    val textRecords = List(
      createSinkRecord(1, users(0), 0, createHeaders[AnyVal](("intheader", 1), ("longheader", 2L))),
      createSinkRecord(1, users(1), 1, createHeaders[AnyVal](("longheader", 2L), ("intheader", 2))),
      createSinkRecord(2, users(2), 2, createHeaders[AnyVal](("intheader", 1), ("longheader", 1L))),
    )
    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.intheader,_header.longheader STOREAS `CSV` WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(3)

    fileList should contain allOf (
      "streamReactorBackups/intheader=1/longheader=2/myTopic(000000000001_000000000000).csv",
      "streamReactorBackups/intheader=2/longheader=2/myTopic(000000000001_000000000001).csv",
      "streamReactorBackups/intheader=1/longheader=1/myTopic(000000000002_000000000002).csv",
    )
  }

  "S3SinkTask" should "partition by whole of kafka message key with key label" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map {
      case (user, k) =>
        new SinkRecord(TopicName, 1, null, (k % 2).toString, schema, user, k.toLong, null, null)
    }

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(6)

    fileList should contain allOf (
      "streamReactorBackups/key=0/myTopic(000000000001_000000000000).csv",
      "streamReactorBackups/key=1/myTopic(000000000001_000000000001).csv",
      "streamReactorBackups/key=0/myTopic(000000000001_000000000002).csv",
      "streamReactorBackups/key=1/myTopic(000000000001_000000000003).csv",
      "streamReactorBackups/key=0/myTopic(000000000001_000000000004).csv",
      "streamReactorBackups/key=1/myTopic(000000000001_000000000005).csv"
    )
  }

  "S3SinkTask" should "partition by whole of kafka message key without key label" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map {
      case (user, k) =>
        new SinkRecord(TopicName, 1, null, (k % 2).toString, schema, user, k.toLong, null, null)
    }

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(6)

    fileList should contain allOf (
      "streamReactorBackups/0/myTopic(000000000001_000000000000).csv",
      "streamReactorBackups/1/myTopic(000000000001_000000000001).csv",
      "streamReactorBackups/0/myTopic(000000000001_000000000002).csv",
      "streamReactorBackups/1/myTopic(000000000001_000000000003).csv",
      "streamReactorBackups/0/myTopic(000000000001_000000000004).csv",
      "streamReactorBackups/1/myTopic(000000000001_000000000005).csv"
    )
  }

  "S3SinkTask" should "partition by custom partitioning with topic/partition/offset" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map {
      case (user, k) =>
        new SinkRecord(TopicName, 1, null, (k % 2).toString, schema, user, k.toLong, null, null)
    }

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _topic, _partition STOREAS `CSV` WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(6)

    fileList should contain allOf (
      "streamReactorBackups/myTopic/000000000001/myTopic(000000000001_000000000000).csv",
      "streamReactorBackups/myTopic/000000000001/myTopic(000000000001_000000000001).csv",
      "streamReactorBackups/myTopic/000000000001/myTopic(000000000001_000000000002).csv",
      "streamReactorBackups/myTopic/000000000001/myTopic(000000000001_000000000003).csv",
      "streamReactorBackups/myTopic/000000000001/myTopic(000000000001_000000000004).csv",
      "streamReactorBackups/myTopic/000000000001/myTopic(000000000001_000000000005).csv"
    )
  }

  "S3SinkTask" should "fail if the key is non-primitive but requests _key partitioning" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map {
      case (user, k) =>
        new SinkRecord(TopicName, 1, null, user, schema, user, k.toLong, null, null)
    }

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val intercepted = intercept[ConnectException] {
      task.put(keyPartitionedRecords.asJava)
    }
    intercepted.getMessage should endWith("Non primitive struct provided, PARTITIONBY _key requested in KCQL")

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(0)

  }

  "S3SinkTask" should "allow a primitive int key for _key partitioning" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map {
      case (user, k) =>
        new SinkRecord(TopicName, 1, null, k % 2, schema, user, k.toLong, null, null)
    }

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(6)

    fileList should contain allOf (
      "streamReactorBackups/0/myTopic(000000000001_000000000000).csv",
      "streamReactorBackups/1/myTopic(000000000001_000000000001).csv",
      "streamReactorBackups/0/myTopic(000000000001_000000000002).csv",
      "streamReactorBackups/1/myTopic(000000000001_000000000003).csv",
      "streamReactorBackups/0/myTopic(000000000001_000000000004).csv",
      "streamReactorBackups/1/myTopic(000000000001_000000000005).csv"
    )
  }

  "S3SinkTask" should "allow partitioning by complex key" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key.region, _key.phonePrefix STOREAS `CSV` WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(3)

    fileList should contain allOf (
      "streamReactorBackups/region=8/phonePrefix=+44/myTopic(000000000001_000000000000).csv",
      "streamReactorBackups/region=5/phonePrefix=+49/myTopic(000000000001_000000000001).csv",
      "streamReactorBackups/region=5/phonePrefix=+49/myTopic(000000000001_000000000002).csv"
    )
  }

  "S3SinkTask" should "not get past kcql parser when contains a slash" in {

    val task = new S3SinkTaskDeprecated()

    val keyWithSlash = "_key.date/of/birth"

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY $keyWithSlash, _key.phonePrefix STOREAS `CSV` WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    intercept[IllegalArgumentException] {
      task.start(props)
    }.getMessage contains "no viable alternative at input"

  }

  "S3SinkTask" should "allow partitioning by complex key and values" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key.region, name WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(3)

    fileList should contain allOf (
      "streamReactorBackups/region=8/name=sam/myTopic(000000000001_000000000000).json",
      "streamReactorBackups/region=5/name=laura/myTopic(000000000001_000000000001).json",
      "streamReactorBackups/region=5/name=tom/myTopic(000000000001_000000000002).json"
    )
  }

  /**
    * This should write partition 1 but not partition 0
    */
  "S3SinkTask" should "write multiple partitions independently" in {

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, schema, users(0), 0),
      toSinkRecord(users(1), 0),
      toSinkRecord(users(2), 1),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
      new TopicPartition(TopicName, 1),
    ).asJava

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 2"),
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(1)

    fileList should contain(
      "streamReactorBackups/myTopic/000000000001/000000000001.json",
    )
  }

  "S3SinkTask" should "process and partition records by a map value" in {

    val map = Map(
      "jedi"     -> 1,
      "klingons" -> 2,
      "cylons"   -> 3,
    ).asJava

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName,
                     0,
                     null,
                     null,
                     SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
                     map,
                     0,
      ),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
    ).asJava

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY jedi WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(1)

    fileList should contain(
      "streamReactorBackups/jedi=1/myTopic(000000000000_000000000000).json",
    )
  }

  "S3SinkTask" should "process and partition records by an array value json" in {

    val array = Array(
      "jedi",
      "klingons",
      "cylons",
    )

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, null, array, 0),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
    ).asJava

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 1"),
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000000/000000000000.json") should be(
      """["jedi","klingons","cylons"]""",
    )

  }

  "S3SinkTask" should "process and partition records by an array value avro" in {

    val array = List(
      "jedi",
      "klingons",
      "cylons",
    ).asJava

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, SchemaBuilder.array(Schema.STRING_SCHEMA), array, 0),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
    ).asJava

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000000/").size should be(1)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000000/000000000000.avro")

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkArray(genericRecords.head.asInstanceOf[GenericData.Array[Utf8]], "jedi", "klingons", "cylons")
  }

  "S3SinkTask" should "process a map of structs avro" in {

    val map = Map(
      "jedi"     -> users(0),
      "klingons" -> users(1),
      "cylons"   -> users(2),
    ).asJava

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, SchemaBuilder.map(Schema.STRING_SCHEMA, schema), map, 0),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
    ).asJava

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000000/").size should be(1)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000000/000000000000.avro")

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)

    val recordsMap = genericRecords.head.asInstanceOf[util.Map[Utf8, GenericData.Record]].asScala

    checkRecord(recordsMap.getOrElse(new Utf8("jedi"), fail("can't find in map")), "sam", Some("mr"), 100.43)
    checkRecord(recordsMap.getOrElse(new Utf8("klingons"), fail("can't find in map")), "laura", Some("ms"), 429.06)
    checkRecord(recordsMap.getOrElse(new Utf8("cylons"), fail("can't find in map")), "tom", None, 395.44)

  }

  "S3SinkTask" should "process a map of structs with nulls avro" in {

    val map = Map(
      "jedi"   -> users(0),
      "cylons" -> null,
    ).asJava

    val optionalSchema: Schema = SchemaBuilder.struct()
      .optional()
      .field("name", SchemaBuilder.string().required().build())
      .field("title", SchemaBuilder.string().optional().build())
      .field("salary", SchemaBuilder.float64().optional().build())
      .build()

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, SchemaBuilder.map(Schema.STRING_SCHEMA, optionalSchema).build(), map, 0),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
    ).asJava

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000000/").size should be(1)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000000/000000000000.avro")

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)

    val recordsMap = genericRecords.head.asInstanceOf[util.Map[Utf8, GenericData.Record]].asScala

    checkRecord(recordsMap.getOrElse(new Utf8("jedi"), fail("can't find in map")), "sam", Some("mr"), 100.43)
    recordsMap.getOrElse(new Utf8("cylons"), fail("can't find in map")) should be(null)

  }

  "S3SinkTask" should "process a map of structs with nulls json" in {

    val keySchema = Schema.STRING_SCHEMA
    val valSchema = SchemaBuilder.struct()
      .optional()
      .field("name", SchemaBuilder.string().required().build())
      .field("title", SchemaBuilder.string().optional().build())
      .field("salary", SchemaBuilder.float64().optional().build())
      .build()
    val mapSchema = SchemaBuilder.map(
      keySchema,
      valSchema,
    ).build()

    val map = Map(
      "jedi"   -> new Struct(valSchema).put("name", "sam").put("title", "mr").put("salary", 100.43),
      "cylons" -> null,
    ).asJava

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, mapSchema, map, 0),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
    ).asJava

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `JSON` WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000000/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000000/000000000000.json") should be(
      """{"jedi":{"name":"sam","title":"mr","salary":100.43},"cylons":null}""",
    )

  }

  "S3SinkTask" should "process and map of arrays avro" in {

    val map = Map(
      "jedi"     -> Array("bob", "john"),
      "klingons" -> Array("joe"),
      "cylons"   -> Array("merv", "marv"),
    ).asJava

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName,
                     0,
                     null,
                     null,
                     SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)),
                     map,
                     0,
      ),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
    ).asJava

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000000/").size should be(1)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000000/000000000000.avro")

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)

    val recordsMap = genericRecords.head.asInstanceOf[util.Map[Utf8, GenericData.Array[Utf8]]].asScala

    checkArray(recordsMap.getOrElse(new Utf8("jedi"), fail("can't find in map")), "bob", "john")
    checkArray(recordsMap.getOrElse(new Utf8("klingons"), fail("can't find in map")), "joe")
    checkArray(recordsMap.getOrElse(new Utf8("cylons"), fail("can't find in map")), "merv", "marv")

  }

  val addressSchema: Schema = SchemaBuilder.struct()
    .field("number", SchemaBuilder.int32().required().build())
    .field("city", SchemaBuilder.string().optional().build())

  val nestedSchema: Schema = SchemaBuilder.struct()
    .field("user", schema)
    .field("address", addressSchema)
    .build()

  val nested: List[Struct] = List(
    new Struct(nestedSchema)
      .put("user",
           new Struct(schema)
             .put("name", "sam")
             .put("title", "mr")
             .put("salary", 100.43),
      )
      .put("address",
           new Struct(addressSchema)
             .put("number", 1)
             .put("city", "bristol"),
      ),
    new Struct(nestedSchema)
      .put("user",
           new Struct(schema)
             .put("name", "laura")
             .put("title", "ms")
             .put("salary", 429.06),
      )
      .put("address",
           new Struct(addressSchema)
             .put("number", 1)
             .put("city", "bristol"),
      ),
    new Struct(nestedSchema)
      .put("user",
           new Struct(schema)
             .put("name", "tom")
             .put("title", null)
             .put("salary", 395.44),
      )
      .put("address",
           new Struct(addressSchema)
             .put("number", 1)
             .put("city", "bristol"),
      ),
  )

  /**
    * This should write partition 1 but not partition 0
    */
  "S3SinkTask" should "partition by nested value fields" in {

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, schema, nested(0), 0),
      toSinkRecord(nested(1), 0),
      toSinkRecord(nested(2), 1),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
      new TopicPartition(TopicName, 1),
    ).asJava

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _value.user.name WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(3)

    fileList should contain allOf (
      "streamReactorBackups/user.name=laura/myTopic(000000000001_000000000000).json",
      "streamReactorBackups/user.name=sam/myTopic(000000000000_000000000000).json",
      "streamReactorBackups/user.name=tom/myTopic(000000000001_000000000001).json",
    )
  }

  /**
    * This should write partition 1 but not partition 0
    */
  "S3SinkTask" should "partition by nested key fields" in {

    val favsSchema: Schema = SchemaBuilder.map(SchemaBuilder.string().build(), SchemaBuilder.string().build())

    val structMapSchema: Schema = SchemaBuilder.struct()
      .field("user", schema)
      .field("favourites", favsSchema)
      .build()

    val nested: List[Struct] = List(
      new Struct(structMapSchema)
        .put("user",
             new Struct(schema)
               .put("name", "sam")
               .put("title", "mr")
               .put("salary", 100.43),
        )
        .put(
          "favourites",
          Map("band" -> "the killers", "film" -> "a clockwork orange").asJava,
        ),
      new Struct(structMapSchema)
        .put("user",
             new Struct(schema)
               .put("name", "laura")
               .put("title", "ms")
               .put("salary", 429.06),
        )
        .put(
          "favourites",
          Map("band" -> "the strokes", "film" -> "a clockwork orange").asJava,
        ),
      new Struct(structMapSchema)
        .put("user",
             new Struct(schema)
               .put("name", "tom")
               .put("title", null)
               .put("salary", 395.44),
        )
        .put(
          "favourites",
          Map().asJava,
        ),
    )

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, structMapSchema, nested(0), SchemaBuilder.int32().build(), 0, 0),
      new SinkRecord(TopicName, 1, structMapSchema, nested(1), SchemaBuilder.int32().build(), 1, 0),
      new SinkRecord(TopicName, 1, structMapSchema, nested(2), SchemaBuilder.int32().build(), 2, 1),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
      new TopicPartition(TopicName, 1),
    ).asJava

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key.favourites.band WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(3)

    fileList should contain allOf (
      "streamReactorBackups/favourites.band=the strokes/myTopic(000000000001_000000000000).json",
      "streamReactorBackups/favourites.band=the killers/myTopic(000000000000_000000000000).json",
      "streamReactorBackups/favourites.band=[missing]/myTopic(000000000001_000000000001).json",
    )
  }

  /**
    * This should write partition 1 but not partition 0
    */
  "S3SinkTask" should "partition by nested header fields" in {

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName,
                     0,
                     null,
                     null,
                     SchemaBuilder.int32().build(),
                     0,
                     0,
                     null,
                     null,
                     createHeaders("header1" -> nested(0)),
      ),
      new SinkRecord(TopicName,
                     1,
                     null,
                     null,
                     SchemaBuilder.int32().build(),
                     1,
                     0,
                     null,
                     null,
                     createHeaders("header1" -> nested(1)),
      ),
      new SinkRecord(TopicName,
                     1,
                     null,
                     null,
                     SchemaBuilder.int32().build(),
                     2,
                     1,
                     null,
                     null,
                     createHeaders("header1" -> nested(2)),
      ),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
      new TopicPartition(TopicName, 1),
    ).asJava

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.header1.user.name WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(3)

    fileList should contain allOf (
      "streamReactorBackups/header1.user.name=laura/myTopic(000000000001_000000000000).json",
      "streamReactorBackups/header1.user.name=sam/myTopic(000000000000_000000000000).json",
      "streamReactorBackups/header1.user.name=tom/myTopic(000000000001_000000000001).json",
    )
  }

  "S3SinkTask" should "flush for every record when configured flush count size of 1 with build local write mode" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql"       -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 1",
          "connect.s3.write.mode" -> "BuildLocal",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(3)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000000.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.json") should be(
      """{"name":"laura","title":"ms","salary":429.06}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000002.json") should be(
      """{"name":"tom","title":null,"salary":395.44}""",
    )

  }

  "S3SinkTask" should "be able to stop when there is no state" in {

    val task = new S3SinkTaskDeprecated()
    task.stop()
  }

  "S3SinkTask" should "flush for every record when configured flush count size of 1 with build local write mode and specifying dir" in {

    val tempDir = Files.createTempDirectory("tempdirtest")

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "name"                           -> "s3SinkTaskBuildLocalTest",
          "connect.s3.local.tmp.directory" -> tempDir.toString,
          "connect.s3.kcql"                -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 1",
          "connect.s3.write.mode"          -> "BuildLocal",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(3)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000000.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.json") should be(
      """{"name":"laura","title":"ms","salary":429.06}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000002.json") should be(
      """{"name":"tom","title":null,"salary":395.44}""",
    )

  }

  "S3SinkTask" should "read from profile" in {

    val profileDir = getResourcesDirectory()
      .getOrElse(fail("cannot get resources dir"))

    val task = new S3SinkTaskDeprecated()

    val props = Map(
      "name"                       -> "sinkName",
      "connect.s3.custom.endpoint" -> uri(),
      PROFILES                     -> s"$profileDir/inttest1.yaml,$profileDir/inttest2.yaml",
      TASK_INDEX                   -> "1:1",
    ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(3)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000000.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001.json") should be(
      """{"name":"laura","title":"ms","salary":429.06}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000002.json") should be(
      """{"name":"tom","title":null,"salary":395.44}""",
    )

  }

  "S3SinkTask" should "not write duplicate offsets" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `avro` WITH_FLUSH_COUNT = 3",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.put(records.asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(1)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000002.avro")

    val genericRecords1 = avroFormatReader.read(bytes)
    genericRecords1.size should be(3)

    // TODO check records
  }

  "S3SinkTask" should "recover after a failure" in {

    val task    = new S3SinkTaskDeprecated()
    val context = mock[SinkTaskContext]
    task.initialize(context)

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql"       -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `json` WITH_FLUSH_COUNT = 3",
          ERROR_POLICY            -> "RETRY",
          ERROR_RETRY_INTERVAL    -> "1",
          HTTP_NBR_OF_RETRIES     -> "2",
          HTTP_SOCKET_TIMEOUT     -> "200",
          HTTP_CONNECTION_TIMEOUT -> "200",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // things going to start failing as our server is down
    pause()

    intercept[RetriableException] {
      task.put(records.asJava)
    }
    intercept[RetriableException] {
      task.put(records.asJava)
    }
    resume()
    task.put(records.asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000002.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}{"name":"tom","title":null,"salary":395.44}""",
    )

  }

  "S3SinkTask" should "recover after a failure for single record commits" in {

    val task    = new S3SinkTaskDeprecated()
    val context = mock[SinkTaskContext]
    task.initialize(context)

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql"       -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `avro` WITH_FLUSH_COUNT = 1",
          ERROR_POLICY            -> "RETRY",
          ERROR_RETRY_INTERVAL    -> "10",
          HTTP_SOCKET_TIMEOUT     -> "200",
          HTTP_CONNECTION_TIMEOUT -> "200",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // things going to start failing as our server is down
    pause()

    intercept[RetriableException] {
      task.put(records.asJava)
    }
    intercept[RetriableException] {
      task.put(records.asJava)
    }
    resume()
    task.put(records.asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(3)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000000.avro")

    val genericRecords1 = avroFormatReader.read(bytes)
    genericRecords1.size should be(1)

  }

  "S3SinkTask" should "continue processing records when a source file has been deleted" in {

    val task    = new S3SinkTaskDeprecated()
    val context = mock[SinkTaskContext]
    task.initialize(context)

    val tmpDir = Files.createTempDirectory("myTestTempDir").toFile

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql"                -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `json` WITH_FLUSH_COUNT = 1",
          ERROR_POLICY                     -> "RETRY",
          ERROR_RETRY_INTERVAL             -> "10",
          HTTP_NBR_OF_RETRIES              -> "5",
          "connect.s3.local.tmp.directory" -> tmpDir.getAbsolutePath,
          HTTP_SOCKET_TIMEOUT              -> "200",
          HTTP_CONNECTION_TIMEOUT          -> "200",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.slice(0, 1).asJava)

    // we need to stop the s3-like-proxy to let records build up
    pause()
    val ex1 = intercept[RetriableException] {
      task.put(records.slice(1, 2).asJava)
    }
    ex1.getMessage should include("Read timed out")

    FileUtils.deleteDirectory(tmpDir)
    tmpDir.exists() should be(false)

    resume()

    task.put(records.slice(0, 3).asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(2)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000000.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
    // file 1 will not exist because it had been deleted before upload.  We continue with the upload regardless.  There will be a message in the log.
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000002.json") should be(
      """{"name":"tom","title":null,"salary":395.44}""",
    )

  }

  "S3SinkTask" should "support multiple topics with wildcard syntax" in {

    val topic2Name = "myTopic2"

    val topic2Records = firstUsers.zipWithIndex.map {
      case (user, k) =>
        toSinkRecord(user, k, topic2Name)
    }

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from `*` WITH_FLUSH_COUNT = 3",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.open(Seq(new TopicPartition(topic2Name, 1)).asJava)
    task.put(records.asJava)
    task.put(topic2Records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.close(Seq(new TopicPartition(topic2Name, 1)).asJava)
    task.stop()

    Seq(TopicName, topic2Name).foreach {
      tName =>
        listBucketPath(BucketName, s"streamReactorBackups/$tName/000000000001/").size should be(1)
        remoteFileAsString(BucketName, s"streamReactorBackups/$tName/000000000001/000000000002.json") should be(
          """
            |{"name":"sam","title":"mr","salary":100.43}
            |{"name":"laura","title":"ms","salary":429.06}
            |{"name":"tom","title":null,"salary":395.44}
            |""".stripMargin.filter(_ >= ' '),
        )
    }

  }

  "S3SinkTask" should "partition by date" in {

    def toTimestamp(str: String): Long =
      LocalDate.parse(str, DateTimeFormatter.ISO_DATE).atTime(6, 0).toInstant(ZoneOffset.UTC).toEpochMilli

    val timeStampedRecords = firstUsers.zip(
      Seq(
        toTimestamp("2010-04-02"),
        toTimestamp("2010-04-02"),
        toTimestamp("2012-06-28"),
      ),
    ).zipWithIndex.map {
      case ((user, timestamp), k) =>
        new SinkRecord(TopicName, 1, null, null, schema, user, k.toLong, timestamp, TimestampType.CREATE_TIME)
    }

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _date.uuuu,_date.LL,_date.dd WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(timeStampedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/") should contain allOf (
      "streamReactorBackups/2010/04/02/myTopic(000000000001_000000000000).json",
      "streamReactorBackups/2010/04/02/myTopic(000000000001_000000000001).json",
      "streamReactorBackups/2012/06/28/myTopic(000000000001_000000000002).json",
    )
  }

  "S3SinkTask" should "write files with topic partition without padding when requested" in {

    val task = new S3SinkTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql"             -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 3",
          "connect.s3.padding.strategy" -> "NoOp",
          "connect.s3.padding.length"   -> "10",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/2.json") should be(
      """
        |{"name":"sam","title":"mr","salary":100.43}
        |{"name":"laura","title":"ms","salary":429.06}
        |{"name":"tom","title":null,"salary":395.44}
        |""".stripMargin.filter(_ >= ' '),
    )

  }

  private def createSinkRecord(partition: Int, valueStruct: Struct, offset: Int, headers: lang.Iterable[Header]) =
    new SinkRecord(TopicName, partition, null, null, null, valueStruct, offset.toLong, null, null, headers)

  private def createKey(keySchema: Schema, keyValuePair: (String, Any)*): Struct = {
    val struct = new Struct(keySchema)
    keyValuePair.foreach {
      case (key: String, value) => struct.put(key, value)
    }
    struct
  }

  private def createHeaders[T](keyValuePair: (String, T)*): lang.Iterable[Header] = {
    val headers = new ConnectHeaders()
    keyValuePair.foreach {
      case (key: String, value) => headers.add(key, value, null)
    }
    headers
  }

  private def openCsvReaderToBucketFile(bucketFileName: String) = {
    val file1Bytes = remoteFileAsBytes(BucketName, bucketFileName)

    val file1Reader = new StringReader(new String(file1Bytes))
    new CSVReader(file1Reader)
  }

  private def getResourcesDirectory(): Try[String] = {
    val url = classOf[S3SinkTaskDeprecatedTest].getResource("/profiles/")
    Try {
      val uri = url.toURI
      logger.info("Profile uri: {}", uri)
      val profilePath = new File(uri).getAbsolutePath
      logger.info("Profile path: {}", profilePath)
      profilePath
    }
  }
}
