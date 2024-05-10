package io.lenses.streamreactor.connect.cloud.common.sink

import com.opencsv.CSVReader
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.ERROR_POLICY_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.MAX_RETRIES_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.RETRY_INTERVAL_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.intf.ConnectionConfig
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushInterval
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushSize
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PartitionIncludeKeys
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.StoreEnvelope
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSinkConfig
import io.lenses.streamreactor.connect.cloud.common.formats.AvroFormatReader
import io.lenses.streamreactor.connect.cloud.common.formats.reader.ParquetFormatReader
import io.lenses.streamreactor.connect.cloud.common.formats.writer.BytesFormatWriter
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.utils.ITSampleSchemaAndData._
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
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
import org.apache.kafka.connect.sink.SinkTask
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers

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
import scala.util.Failure
import scala.util.Success
import scala.util.Try
abstract class CoreSinkTaskTestCases[
  MD <: FileMetadata,
  SI <: StorageInterface[MD],
  C <: CloudSinkConfig[CC],
  CC <: ConnectionConfig,
  CT,
  T <: CloudSinkTask[MD, C, CC, CT],
](unitUnderTest: String,
) extends CloudPlatformEmulatorSuite[MD, SI, C, CC, CT, T]
    with Matchers
    with MockitoSugar
    with LazyLogging {

  private val context = mock[SinkTaskContext]

  private val parquetFormatReader = new ParquetFormatReader()
  private val avroFormatReader    = new AvroFormatReader()

  protected val PrefixName = "streamReactorBackups"
  protected val TopicName  = "myTopic"

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
                     k.toLong,
                     TimestampType.CREATE_TIME,
                     createHeaders(("headerPartitionKey", (k % 2).toString)),
      )
  }

  private val records = firstUsers.zipWithIndex.map {
    case (user, k) =>
      toSinkRecord(user, k)
  }

  private def toSinkRecord(user: Struct, k: Int, topicName: String = TopicName) =
    new SinkRecord(topicName, 1, null, null, schema, user, k.toLong, k.toLong, TimestampType.CREATE_TIME)

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
                   0,
                   TimestampType.CREATE_TIME,
    ),
    new SinkRecord(TopicName,
                   1,
                   null,
                   createKey(keySchema, ("phonePrefix", "+49"), ("region", 5)),
                   null,
                   users(1),
                   1,
                   1,
                   TimestampType.CREATE_TIME,
    ),
    new SinkRecord(TopicName,
                   1,
                   null,
                   createKey(keySchema, ("phonePrefix", "+49"), ("region", 5)),
                   null,
                   users(2),
                   2,
                   2,
                   TimestampType.CREATE_TIME,
    ),
  )

  unitUnderTest should "flush on configured flush time intervals" in {

    val task = createSinkTask()

    val props = (defaultProps
      +
        (
          s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PROPERTIES( '${FlushInterval.entryName}' = 1)",
        ),
    ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)

    listBucketPath(BucketName, "/streamReactorBackups/myTopic/1").size should be(0)

    Thread.sleep(1200) // wait for 1000 millisecond so the next call to put will cause a flush

    task.put(Seq().asJava)
    task.put(records.asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/000000000002_0_2.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}{"name":"tom","title":null,"salary":395.44}""",
    )

  }

  unitUnderTest should "flush for every record when configured flush count size of 1" in {

    val props =
      (defaultProps + (s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PROPERTIES('${FlushCount.entryName}'=1)")).asJava

    val task = createTask(context, props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(3)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/000000000000_0_0.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/000000000001_1_1.json") should be(
      """{"name":"laura","title":"ms","salary":429.06}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/000000000002_2_2.json") should be(
      """{"name":"tom","title":null,"salary":395.44}""",
    )

  }

  /**
    * The file sizes of the 3 records above come out as 44,46,44.
    * We're going to set the threshold to 80 - so once we've written 2 records to a file then the
    * second file should only contain a single record.  This second file won't have been written yet.
    */
  unitUnderTest should "flush on configured file size" in {

    val props =
      defaultProps + (s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PROPERTIES('${FlushSize.entryName}'=80)")

    val task = createTask(context, props.asJava)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/000000000001_0_1.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}""",
    )

  }

  unitUnderTest should "flush on configured file size for Parquet" in {

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `Parquet` PROPERTIES('${FlushSize.entryName}'=20)",
    ),
    ).asJava

    val task = createTask(context, props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(2)
    getMetadata(BucketName, "streamReactorBackups/myTopic/1/000000000000_0_0.parquet").size should be >= 941L
    getMetadata(BucketName, "streamReactorBackups/myTopic/1/000000000001_1_1.parquet").size should be >= 954L

    var genericRecords =
      parquetFormatReader.read(remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000000_0_0.parquet"))
    genericRecords.size should be(1)
    checkRecord(genericRecords.head, "sam", "mr", 100.43)

    genericRecords =
      parquetFormatReader.read(remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000001_1_1.parquet"))
    genericRecords.size should be(1)
    checkRecord(genericRecords.head, "laura", "ms", 429.06)

  }

  def createTask(context: SinkTaskContext, props: util.Map[String, String]): SinkTask = {
    reset(context)
    val task: SinkTask = createSinkTask()
    task.initialize(context)
    task.start(props)
    task
  }

  /**
    * The difference in this test is that the sink is opened again, which will cause the offsets to be copied to the
    * context
    */
  unitUnderTest should "put existing offsets to the context" in {

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PROPERTIES('${FlushCount.entryName}'=1)",
    ),
    ).asJava

    val task = createTask(context, props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(3)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/000000000000_0_0.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/000000000001_1_1.json") should be(
      """{"name":"laura","title":"ms","salary":429.06}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/000000000002_2_2.json") should be(
      """{"name":"tom","title":null,"salary":395.44}""",
    )

    verify(context).offset(new TopicPartition("myTopic", 1), 2)
  }

  unitUnderTest should "skip when kafka connect resends the same offsets after opening" in {

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `Parquet` PROPERTIES('${FlushCount.entryName}'=2)",
    )).asJava

    var task: SinkTask = createTask(context, props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    verify(context, never).offset(new TopicPartition("myTopic", 1), 0)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition("myTopic", 1)).asJava)
    task.stop()

    val list = listBucketPath(BucketName, "streamReactorBackups/myTopic/1/")
    list.size should be(1)
    list should contain("streamReactorBackups/myTopic/1/000000000001_0_1.parquet")

    val modificationDate =
      getMetadata(BucketName, "streamReactorBackups/myTopic/1/000000000001_0_1.parquet").lastModified

    task = createTask(context, props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    verify(context).offset(new TopicPartition("myTopic", 1), 1)
    task.put(records.asJava)

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    // file should not have been overwritten
    getMetadata(BucketName, "streamReactorBackups/myTopic/1/000000000001_0_1.parquet").lastModified should be(
      modificationDate,
    )

    task.close(Seq(new TopicPartition("myTopic", 1)).asJava)
    task.stop()

    task = createTask(context, props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    verify(context).offset(new TopicPartition("myTopic", 1), 1)
    // only 1 "real" record so should leave it hanging again
    task.put(List(records(1), records(2)).asJava)

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    // file should not have been overwritten
    getMetadata(BucketName, "streamReactorBackups/myTopic/1/000000000001_0_1.parquet").lastModified should be(
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

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(2)

    // file should not have been overwritten
    getMetadata(BucketName, "streamReactorBackups/myTopic/1/000000000001_0_1.parquet").lastModified should be(
      modificationDate,
    )

    task.close(Seq(new TopicPartition("myTopic", 1)).asJava)
    task.stop()

  }

  unitUnderTest should "write to parquet format not using the envelope data storage" in {

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"""insert into $BucketName:$PrefixName select * from $TopicName STOREAS PARQUET PROPERTIES('${FlushCount.entryName}'=1)""",
    )).asJava
    val task = createTask(context, props)

    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(3)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000000_0_0.parquet")

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords.head, "sam", "mr", 100.43)

  }

  unitUnderTest should "write to parquet format using the envelope data storage" in {

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"""insert into $BucketName:$PrefixName select * from $TopicName STOREAS PARQUET PROPERTIES('${FlushCount.entryName}'=1, '${StoreEnvelope.entryName}'= true)""",
    )).asJava
    val task = createTask(context, props)

    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(3)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000000_0_0.parquet")

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords.head.get("value").asInstanceOf[GenericRecord], "sam", "mr", 100.43)
  }

  unitUnderTest should "write to avro format" in {

    val task = createSinkTask()

    val props = (
      defaultProps + (s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` PROPERTIES('${FlushCount.entryName}'=1)")
    ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(3)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000000_0_0.avro")

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords.head, "sam", "mr", 100.43)

  }

  unitUnderTest should "write to avro format using the envelope data storage" in {

    val task = createSinkTask()

    val props = (
      defaultProps + (s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` PROPERTIES('${FlushCount.entryName}'=1, '${StoreEnvelope.entryName}'= true)")
    ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(3)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000000_0_0.avro")

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords.head.get("value").asInstanceOf[GenericRecord], "sam", "mr", 100.43)

  }

  unitUnderTest should "error when trying to write AVRO to text format" in {

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `TEXT` PROPERTIES('${FlushCount.entryName}'=2)",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    assertThrows[ConnectException] {
      task.put(records.asJava)
    }

    task.stop()
  }

  unitUnderTest should "write to text format" in {

    val textRecords = List(
      new SinkRecord(TopicName, 1, null, null, null, "Sausages", 0, 0, TimestampType.CREATE_TIME),
      new SinkRecord(TopicName, 1, null, null, null, "Mash", 1, 1, TimestampType.CREATE_TIME),
      new SinkRecord(TopicName, 1, null, null, null, "Peas", 2, 2, TimestampType.CREATE_TIME),
      new SinkRecord(TopicName, 1, null, null, null, "Gravy", 3, 3, TimestampType.CREATE_TIME),
    )
    val task = createSinkTask()

    val props =
      (defaultProps + (s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `TEXT` PROPERTIES('${FlushCount.entryName}'=2)")).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(2)

    val file1Bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000001_0_1.text")
    new String(file1Bytes) should be("Sausages\nMash\n")

    val file2Bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000003_2_3.text")
    new String(file2Bytes) should be("Peas\nGravy\n")

  }

  unitUnderTest should "write to csv format with headers" in {

    val task = createSinkTask()

    val extraRecord = toSinkRecord(new Struct(schema).put("name", "bob").put("title", "mr").put("salary", 200.86), 3)

    val allRecords: List[SinkRecord] = records :+ extraRecord

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `CSV_WITHHEADERS` PROPERTIES('${FlushCount.entryName}'=2)",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(allRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(2)

    val file1Bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000001_0_1.csv")

    val file1Reader    = new StringReader(new String(file1Bytes))
    val file1CsvReader = new CSVReader(file1Reader)

    file1CsvReader.readNext() should be(Array("name", "title", "salary"))
    file1CsvReader.readNext() should be(Array("sam", "mr", "100.43"))
    file1CsvReader.readNext() should be(Array("laura", "ms", "429.06"))
    file1CsvReader.readNext() should be(null)

    val file2Bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000003_2_3.csv")

    val file2Reader    = new StringReader(new String(file2Bytes))
    val file2CsvReader = new CSVReader(file2Reader)

    file2CsvReader.readNext() should be(Array("name", "title", "salary"))
    file2CsvReader.readNext() should be(Array("tom", "", "395.44"))
    file2CsvReader.readNext() should be(Array("bob", "mr", "200.86"))
    file2CsvReader.readNext() should be(null)
  }

  unitUnderTest should "write to csv format without headers" in {

    val task = createSinkTask()

    val extraRecord = toSinkRecord(new Struct(schema).put("name", "bob").put("title", "mr").put("salary", 200.86), 3)

    val allRecords: List[SinkRecord] = records :+ extraRecord

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=2)",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(allRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(2)

    val file1Bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000001_0_1.csv")

    val file1Reader    = new StringReader(new String(file1Bytes))
    val file1CsvReader = new CSVReader(file1Reader)

    file1CsvReader.readNext() should be(Array("sam", "mr", "100.43"))
    file1CsvReader.readNext() should be(Array("laura", "ms", "429.06"))
    file1CsvReader.readNext() should be(null)

    val file2Bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000003_2_3.csv")

    val file2Reader    = new StringReader(new String(file2Bytes))
    val file2CsvReader = new CSVReader(file2Reader)

    file2CsvReader.readNext() should be(Array("tom", "", "395.44"))
    file2CsvReader.readNext() should be(Array("bob", "mr", "200.86"))
    file2CsvReader.readNext() should be(null)
  }

  unitUnderTest should "use custom partitioning scheme and flush for every record" in {

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name,title,salary PROPERTIES('${FlushCount.entryName}'=1)",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(headerPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/").size should be(6)

    remoteFileAsString(
      BucketName,
      "streamReactorBackups/name=first/title=primary/salary=[missing]/myTopic(1_000000000000).json",
    ) should be("""{"name":"first","title":"primary","salary":null}""")
    remoteFileAsString(
      BucketName,
      "streamReactorBackups/name=second/title=secondary/salary=100.0/myTopic(1_000000000001).json",
    ) should be("""{"name":"second","title":"secondary","salary":100.0}""")
    remoteFileAsString(
      BucketName,
      "streamReactorBackups/name=third/title=primary/salary=100.0/myTopic(1_000000000002).json",
    ) should be("""{"name":"third","title":"primary","salary":100.0}""")
    remoteFileAsString(
      BucketName,
      "streamReactorBackups/name=first/title=[missing]/salary=200.0/myTopic(1_000000000003).json",
    ) should be("""{"name":"first","title":null,"salary":200.0}""")
    remoteFileAsString(
      BucketName,
      "streamReactorBackups/name=second/title=[missing]/salary=100.0/myTopic(1_000000000004).json",
    ) should be("""{"name":"second","title":null,"salary":100.0}""")
    remoteFileAsString(
      BucketName,
      "streamReactorBackups/name=third/title=[missing]/salary=100.0/myTopic(1_000000000005).json",
    ) should be("""{"name":"third","title":null,"salary":100.0}""")

  }

  /**
    * As soon as one file is eligible for writing, it will write all those from the same topic partition.  Therefore 4
    * files are written instead of 2, as there are 2 points at which the write is triggered and the half-full files must
    * be written as well as those reaching the threshold.
    */
  unitUnderTest should "use custom partitioning scheme and flush after two written records" in {

    val task = createSinkTask()

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

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name,title PROPERTIES('${FlushCount.entryName}'=2)",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/").size should be(4)

    remoteFileAsString(BucketName,
                       "streamReactorBackups/name=first/title=primary/myTopic(1_000000000002).json",
    ) should be(
      """{"name":"first","title":"primary","salary":null}{"name":"first","title":"primary","salary":100.0}""",
    )
    remoteFileAsString(BucketName,
                       "streamReactorBackups/name=first/title=secondary/myTopic(1_000000000001).json",
    ) should be(
      """{"name":"first","title":"secondary","salary":100.0}""",
    )
    remoteFileAsString(BucketName,
                       "streamReactorBackups/name=second/title=secondary/myTopic(1_000000000005).json",
    ) should be(
      """{"name":"second","title":"secondary","salary":200.0}{"name":"second","title":"secondary","salary":100.0}""",
    )
    remoteFileAsString(BucketName,
                       "streamReactorBackups/name=second/title=primary/myTopic(1_000000000004).json",
    ) should be(
      """{"name":"second","title":"primary","salary":100.0}""",
    )

  }

  unitUnderTest should "use custom partitioning with value display only" in {

    val task = createSinkTask()

    val props =
      defaultProps + (s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name,title,salary PROPERTIES('${FlushCount.entryName}'=1,'${PartitionIncludeKeys.entryName}'=false)")

    task.start(props.asJava)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(headerPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/").size should be(6)

    remoteFileAsString(BucketName,
                       "streamReactorBackups/first/primary/[missing]/myTopic(1_000000000000).json",
    ) should be(
      """{"name":"first","title":"primary","salary":null}""",
    )
    remoteFileAsString(BucketName,
                       "streamReactorBackups/second/secondary/100.0/myTopic(1_000000000001).json",
    ) should be(
      """{"name":"second","title":"secondary","salary":100.0}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/third/primary/100.0/myTopic(1_000000000002).json") should be(
      """{"name":"third","title":"primary","salary":100.0}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/first/[missing]/200.0/myTopic(1_000000000003).json") should be(
      """{"name":"first","title":null,"salary":200.0}""",
    )
    remoteFileAsString(BucketName,
                       "streamReactorBackups/second/[missing]/100.0/myTopic(1_000000000004).json",
    ) should be(
      """{"name":"second","title":null,"salary":100.0}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/third/[missing]/100.0/myTopic(1_000000000005).json") should be(
      """{"name":"third","title":null,"salary":100.0}""",
    )

  }

  unitUnderTest should "writing to bytes format doesn't allow you to combine files" in {

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `BYTES` PROPERTIES('${FlushCount.entryName}'=2)",
    )).asJava

    Try(task.start(props)) match {
      case Failure(exception) =>
        exception.getMessage should startWith(s"${FlushCount.entryName} > 1 is not allowed for BYTES")
      case Success(_) => fail("Exception expected")
    }
    Try(task.stop())
  }

  unitUnderTest should "write to bytes format" in {

    val stream = classOf[BytesFormatWriter].getResourceAsStream("/streamreactor-logo.png")
    val bytes: Array[Byte] = IOUtils.toByteArray(stream)

    val textRecords = List(
      new SinkRecord(TopicName, 1, null, null, null, bytes, 0, 0, TimestampType.CREATE_TIME),
    )
    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `BYTES` PROPERTIES('${FlushCount.entryName}'=1)",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000000_0_0.bytes") should be(bytes)

  }

  unitUnderTest should "prompt user of defaults for bytes format to specify a flush count" in {

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `BYTES`",
    )).asJava

    Try(task.start(props)) match {
      case Failure(exception) =>
        exception.getMessage should endWith(
          s"If you are using BYTES but not specified a ${FlushCount.entryName}, then do so by adding ${FlushCount.entryName} = 1 to your KCQL PROPERTIES section.",
        )
      case Success(_) => fail("Exception expected")
    }
    Try(task.stop())
  }

  unitUnderTest should "support header values for data partitioning" in {

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
    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.phonePrefix,_header.region STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1)",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/").size should be(3)

    val file1CsvReader: CSVReader =
      openCsvReaderToBucketFile("streamReactorBackups/phonePrefix=+44/region=8/myTopic(1_000000000000).csv")
    file1CsvReader.readNext() should be(Array("sam", "mr", "100.43"))
    file1CsvReader.readNext() should be(null)

    val file2CsvReader: CSVReader =
      openCsvReaderToBucketFile("streamReactorBackups/phonePrefix=+49/region=5/myTopic(1_000000000001).csv")
    file2CsvReader.readNext() should be(Array("laura", "ms", "429.06"))
    file2CsvReader.readNext() should be(null)

    val file3CsvReader: CSVReader =
      openCsvReaderToBucketFile("streamReactorBackups/phonePrefix=+49/region=5/myTopic(1_000000000002).csv")
    file3CsvReader.readNext() should be(Array("tom", "", "395.44"))
    file3CsvReader.readNext() should be(null)
  }

  unitUnderTest should "combine header and value-extracted partition" in {

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.headerPartitionKey,_value.name STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1)",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(headerPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(6)

    fileList should contain allOf (
      "streamReactorBackups/headerPartitionKey=0/name=first/myTopic(1_000000000000).csv",
      "streamReactorBackups/headerPartitionKey=1/name=second/myTopic(1_000000000001).csv",
      "streamReactorBackups/headerPartitionKey=0/name=third/myTopic(1_000000000002).csv",
      "streamReactorBackups/headerPartitionKey=1/name=first/myTopic(1_000000000003).csv",
      "streamReactorBackups/headerPartitionKey=0/name=second/myTopic(1_000000000004).csv",
      "streamReactorBackups/headerPartitionKey=1/name=third/myTopic(1_000000000005).csv"
    )
  }

  unitUnderTest should "fail when header value is missing from the record when header partitioning activated" in {

    val textRecords = List(
      createSinkRecord(1, users(0), 0, createHeaders(("feet", "5"))),
      createSinkRecord(1, users(1), 0, createHeaders(("hair", "blue"), ("feet", "5"))),
    )

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.hair,_header.feet STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1)",
    )).asJava

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

  unitUnderTest should "support numeric header data types" in {

    val textRecords = List(
      createSinkRecord(1, users(0), 0, createHeaders[AnyVal](("intheader", 1), ("longheader", 2L))),
      createSinkRecord(1, users(1), 1, createHeaders[AnyVal](("longheader", 2L), ("intheader", 2))),
      createSinkRecord(2, users(2), 2, createHeaders[AnyVal](("intheader", 1), ("longheader", 1L))),
    )
    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.intheader,_header.longheader STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1)",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(3)

    fileList should contain allOf (
      "streamReactorBackups/intheader=1/longheader=2/myTopic(1_000000000000).csv",
      "streamReactorBackups/intheader=2/longheader=2/myTopic(1_000000000001).csv",
      "streamReactorBackups/intheader=1/longheader=1/myTopic(2_000000000002).csv",
    )
  }

  unitUnderTest should "partition by whole of kafka message key with key label" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map {
      case (user, k) =>
        new SinkRecord(TopicName, 1, null, (k % 2).toString, schema, user, k.toLong, null, null)
    }

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1)",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(6)

    fileList should contain allOf (
      "streamReactorBackups/key=0/myTopic(1_000000000000).csv",
      "streamReactorBackups/key=1/myTopic(1_000000000001).csv",
      "streamReactorBackups/key=0/myTopic(1_000000000002).csv",
      "streamReactorBackups/key=1/myTopic(1_000000000003).csv",
      "streamReactorBackups/key=0/myTopic(1_000000000004).csv",
      "streamReactorBackups/key=1/myTopic(1_000000000005).csv"
    )
  }

  unitUnderTest should "partition by whole of kafka message key without key label" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map {
      case (user, k) =>
        new SinkRecord(TopicName, 1, null, (k % 2).toString, schema, user, k.toLong, null, null)
    }

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1,'${PartitionIncludeKeys.entryName}'=false)",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(6)

    fileList should contain allOf (
      "streamReactorBackups/0/myTopic(1_000000000000).csv",
      "streamReactorBackups/1/myTopic(1_000000000001).csv",
      "streamReactorBackups/0/myTopic(1_000000000002).csv",
      "streamReactorBackups/1/myTopic(1_000000000003).csv",
      "streamReactorBackups/0/myTopic(1_000000000004).csv",
      "streamReactorBackups/1/myTopic(1_000000000005).csv"
    )
  }

  unitUnderTest should "partition by custom partitioning with topic/partition/offset" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map {
      case (user, k) =>
        new SinkRecord(TopicName, 1, null, (k % 2).toString, schema, user, k.toLong, null, null)
    }

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _topic, _partition STOREAS `CSV` PROPERTIES('padding.length.partition'='12', 'padding.length.offset'='12','${FlushCount.entryName}'=1,'${PartitionIncludeKeys.entryName}'=false)",
    )).asJava

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

  unitUnderTest should "fail if the key is non-primitive but requests _key partitioning" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map {
      case (user, k) =>
        new SinkRecord(TopicName, 1, null, user, schema, user, k.toLong, null, null)
    }

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1)",
    )).asJava

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

  unitUnderTest should "allow a primitive int key for _key partitioning" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map {
      case (user, k) =>
        new SinkRecord(TopicName, 1, null, k % 2, schema, user, k.toLong, null, null)
    }

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` PROPERTIES('padding.length.partition'='12', 'padding.length.offset'='12', '${FlushCount.entryName}'=1,'${PartitionIncludeKeys.entryName}'=false)",
    )).asJava

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

  unitUnderTest should "allow partitioning by complex key" in {

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key.region, _key.phonePrefix STOREAS `CSV` PROPERTIES('padding.length.partition'='12', 'padding.length.offset'='12', '${FlushCount.entryName}'=1)",
    )).asJava

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

  unitUnderTest should "allow partitioning by complex key and values" in {

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key.region, name PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
    )).asJava

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

  unitUnderTest should "write to root without a prefix" in {

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName select * from $TopicName PARTITIONBY _key.region, name PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = listBucketPath(BucketName, "")
    fileList.size should be(4)

    // the results do contain the index.  The sink always looks for the index at the root of the bucket when offset synching.
    // The source excludes the index files.
    fileList should contain allOf (
      ".indexes/gcpSinkTaskTest/myTopic/00001/00000000000000000002",
      "region=8/name=sam/myTopic(000000000001_000000000000).json",
      "region=5/name=laura/myTopic(000000000001_000000000001).json",
      "region=5/name=tom/myTopic(000000000001_000000000002).json"
    )
  }

  /**
    * This should write partition 1 but not partition 0
    */
  unitUnderTest should "write multiple partitions independently" in {

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, schema, users(0), 0, 0, TimestampType.CREATE_TIME),
      toSinkRecord(users(1), 0),
      toSinkRecord(users(2), 1),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
      new TopicPartition(TopicName, 1),
    ).asJava

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PROPERTIES('${FlushCount.entryName}'=2,'padding.length.partition'='12', 'padding.length.offset'='12')",
    )).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(1)

    fileList should contain(
      "streamReactorBackups/myTopic/000000000001/000000000001_0_1.json",
    )
  }

  unitUnderTest should "process and partition records by a map value" in {

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

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY jedi PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
    )).asJava

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

  unitUnderTest should "process and partition records by an array value json" in {

    val array = Array(
      "jedi",
      "klingons",
      "cylons",
    )

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, null, array, 0, 2, TimestampType.CREATE_TIME),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
    ).asJava

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
    )).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000000/000000000000_2_2.json") should be(
      """["jedi","klingons","cylons"]""",
    )

  }

  unitUnderTest should "process and partition records by an array value avro" in {

    val array = List(
      "jedi",
      "klingons",
      "cylons",
    ).asJava

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName,
                     0,
                     null,
                     null,
                     SchemaBuilder.array(Schema.STRING_SCHEMA),
                     array,
                     0,
                     2,
                     TimestampType.CREATE_TIME,
      ),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
    ).asJava

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
    )).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000000/").size should be(1)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000000/000000000000_2_2.avro")

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkArray(genericRecords.head.asInstanceOf[GenericData.Array[Utf8]], "jedi", "klingons", "cylons")
  }

  unitUnderTest should "process a map of structs avro" in {

    val map = Map(
      "jedi"     -> users(0),
      "klingons" -> users(1),
      "cylons"   -> users(2),
    ).asJava

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName,
                     0,
                     null,
                     null,
                     SchemaBuilder.map(Schema.STRING_SCHEMA, schema),
                     map,
                     0,
                     2,
                     TimestampType.CREATE_TIME,
      ),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
    ).asJava

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
    )).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000000/").size should be(1)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000000/000000000000_2_2.avro")

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)

    val recordsMap = genericRecords.head.asInstanceOf[util.Map[Utf8, GenericData.Record]].asScala

    checkRecord(recordsMap.getOrElse(new Utf8("jedi"), fail("can't find in map")), "sam", Some("mr"), 100.43)
    checkRecord(recordsMap.getOrElse(new Utf8("klingons"), fail("can't find in map")), "laura", Some("ms"), 429.06)
    checkRecord(recordsMap.getOrElse(new Utf8("cylons"), fail("can't find in map")), "tom", None, 395.44)

  }

  unitUnderTest should "process a map of structs with nulls avro" in {

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
      new SinkRecord(TopicName,
                     0,
                     null,
                     null,
                     SchemaBuilder.map(Schema.STRING_SCHEMA, optionalSchema).build(),
                     map,
                     0,
                     2,
                     TimestampType.CREATE_TIME,
      ),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
    ).asJava

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
    )).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000000/").size should be(1)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000000/000000000000_2_2.avro")

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)

    val recordsMap = genericRecords.head.asInstanceOf[util.Map[Utf8, GenericData.Record]].asScala

    checkRecord(recordsMap.getOrElse(new Utf8("jedi"), fail("can't find in map")), "sam", Some("mr"), 100.43)
    recordsMap.getOrElse(new Utf8("cylons"), fail("can't find in map")) should be(null)

  }

  unitUnderTest should "process a map of structs with nulls json" in {

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
      new SinkRecord(TopicName, 0, null, null, mapSchema, map, 0, 1, TimestampType.CREATE_TIME),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
    ).asJava

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `JSON` PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
    )).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000000/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000000/000000000000_1_1.json") should be(
      """{"jedi":{"name":"sam","title":"mr","salary":100.43},"cylons":null}""",
    )

  }

  unitUnderTest should "process and map of arrays avro" in {

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
                     0,
                     TimestampType.CREATE_TIME,
      ),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
    ).asJava

    val task = createSinkTask()

    val props = (defaultProps ++
      Map(
        s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
      )).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000000/").size should be(1)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000000/000000000000_0_0.avro")

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
  unitUnderTest should "partition by nested value fields" in {

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, schema, nested(0), 0),
      toSinkRecord(nested(1), 0),
      toSinkRecord(nested(2), 1),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
      new TopicPartition(TopicName, 1),
    ).asJava

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _value.user.name PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
    )).asJava

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
  unitUnderTest should "partition by nested key fields" in {

    val favsSchema: Schema = SchemaBuilder.map(SchemaBuilder.string().build(), SchemaBuilder.string().build())

    val structMapSchema: Schema = SchemaBuilder.struct()
      .field("user", schema)
      .field("favourites", favsSchema)
      .field("cost.centre.id", SchemaBuilder.string())
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
        )
        .put(
          "cost.centre.id",
          "100",
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
        )
        .put(
          "cost.centre.id",
          "200",
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
        )
        .put(
          "cost.centre.id",
          "100",
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

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key.favourites.band, _key.`cost.centre.id` PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
    )).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = listBucketPath(BucketName, "streamReactorBackups/")
    fileList.size should be(3)

    fileList should contain allOf (
      "streamReactorBackups/favourites.band=the strokes/cost.centre.id=200/myTopic(000000000001_000000000000).json",
      "streamReactorBackups/favourites.band=the killers/cost.centre.id=100/myTopic(000000000000_000000000000).json",
      "streamReactorBackups/favourites.band=[missing]/cost.centre.id=100/myTopic(000000000001_000000000001).json",
    )
  }

  /**
    * This should write partition 1 but not partition 0
    */
  unitUnderTest should "partition by nested header fields" in {

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

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.header1.user.name PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
    )).asJava

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

  unitUnderTest should "flush for every record when configured flush count size of 1 with build local write mode" in {

    val task = createSinkTask()

    val props = (defaultProps
      ++
        Map(
          s"$prefix.kcql"       -> s"insert into $BucketName:$PrefixName select * from $TopicName PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
          s"$prefix.write.mode" -> "BuildLocal",
        ),
    ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(3)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000000_0_0.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001_1_1.json") should be(
      """{"name":"laura","title":"ms","salary":429.06}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000002_2_2.json") should be(
      """{"name":"tom","title":null,"salary":395.44}""",
    )

  }

  unitUnderTest should "be able to stop when there is no state" in {

    val task = createSinkTask()
    task.stop()
  }

  unitUnderTest should "flush for every record when configured flush count size of 1 with build local write mode and specifying dir" in {

    val tempDir = Files.createTempDirectory("tempdirtest")

    val task = createSinkTask()

    val props = (defaultProps ++
      Map(
        "name"                         -> "gcpSinkTaskTest",
        s"$prefix.local.tmp.directory" -> tempDir.toString,
        s"$prefix.kcql"                -> s"insert into $BucketName:$PrefixName select * from $TopicName PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
        s"$prefix.write.mode"          -> "BuildLocal",
      ),
    ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(3)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000000_0_0.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000001_1_1.json") should be(
      """{"name":"laura","title":"ms","salary":429.06}""",
    )
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000002_2_2.json") should be(
      """{"name":"tom","title":null,"salary":395.44}""",
    )

  }

  unitUnderTest should "not write duplicate offsets" in {

    val task = createSinkTask()

    val props = (defaultProps +
      (
        s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `avro` PROPERTIES('${FlushCount.entryName}'=3)",
      )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.put(records.asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000002_0_2.avro")

    val genericRecords1 = avroFormatReader.read(bytes)
    genericRecords1.size should be(3)

    // TODO check records
  }

  unitUnderTest should "recover after a failure" in {

    val task = createSinkTask()
    task.initialize(context)

    val props = (defaultProps ++
      Map(
        s"$prefix.kcql"                          -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `json` PROPERTIES('${FlushCount.entryName}'=3)",
        s"$prefix.$ERROR_POLICY_PROP_SUFFIX"     -> "RETRY",
        s"$prefix.$RETRY_INTERVAL_PROP_SUFFIX"   -> "1",
        s"$prefix.http.$MAX_RETRIES_PROP_SUFFIX" -> "2",
        s"$prefix.http.socket.timeout"           -> "200",
        s"$prefix.http.connection.timeout"       -> "200",
      )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // things going to start failing as our server is down
    container.pause()

    intercept[RetriableException] {
      task.put(records.asJava)
    }
    intercept[RetriableException] {
      task.put(records.asJava)
    }
    container.resume()
    task.put(records.asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/000000000002_0_2.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}{"name":"tom","title":null,"salary":395.44}""",
    )

  }

  unitUnderTest should "recover after a failure for single record commits" in {

    val task = createSinkTask()
    task.initialize(context)

    val props = (defaultProps ++
      Map(
        s"$prefix.kcql"                        -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `avro` PROPERTIES('${FlushCount.entryName}'=1)",
        s"$prefix.$ERROR_POLICY_PROP_SUFFIX"   -> "RETRY",
        s"$prefix.$RETRY_INTERVAL_PROP_SUFFIX" -> "10",
        s"$prefix.http.socket.timeout"         -> "200",
        s"$prefix.http.connection.timeout"     -> "200",
      ))
      .asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // things going to start failing as our server is down
    container.pause()

    intercept[RetriableException] {
      task.put(records.asJava)
    }
    intercept[RetriableException] {
      task.put(records.asJava)
    }
    container.resume()
    task.put(records.asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(3)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/1/000000000000_0_0.avro")

    val genericRecords1 = avroFormatReader.read(bytes)
    genericRecords1.size should be(1)

  }

  unitUnderTest should "continue processing records when a source file has been deleted" in {

    val task = createSinkTask()
    task.initialize(context)

    val tmpDir = Files.createTempDirectory("myTestTempDir").toFile

    val props = (defaultProps ++
      Map(
        s"$prefix.kcql"                          -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `json` PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12')",
        s"$prefix.$ERROR_POLICY_PROP_SUFFIX"     -> "RETRY",
        s"$prefix.$RETRY_INTERVAL_PROP_SUFFIX"   -> "10",
        s"$prefix.http.$MAX_RETRIES_PROP_SUFFIX" -> "5",
        s"$prefix.local.tmp.directory"           -> tmpDir.getAbsolutePath,
        s"$prefix.http.socket.timeout"           -> "200",
        s"$prefix.http.connection.timeout"       -> "200",
      ),
    ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.slice(0, 1).asJava)

    // we need to stop the gcp-like-proxy to let records build up
    container.pause()
    val ex1 = intercept[RetriableException] {
      task.put(records.slice(1, 2).asJava)
    }
    ex1.getMessage should include("Read timed out")

    FileUtils.deleteDirectory(tmpDir)
    tmpDir.exists() should be(false)

    container.resume()

    task.put(records.slice(0, 3).asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(2)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000000_0_0.json") should be(
      """{"name":"sam","title":"mr","salary":100.43}""",
    )
    // file 1 will not exist because it had been deleted before upload.  We continue with the upload regardless.  There will be a message in the log.
    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/000000000001/000000000002_2_2.json") should be(
      """{"name":"tom","title":null,"salary":395.44}""",
    )

  }

  unitUnderTest should "support multiple topics with wildcard syntax" in {

    val topic2Name = "myTopic2"

    val topic2Records = firstUsers.zipWithIndex.map {
      case (user, k) =>
        toSinkRecord(user, k, topic2Name)
    }

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from `*` PROPERTIES('${FlushCount.entryName}'=3,'padding.length.partition'='12', 'padding.length.offset'='12')",
    )).asJava

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
        remoteFileAsString(BucketName, s"streamReactorBackups/$tName/000000000001/000000000002_0_2.json") should be(
          """
            |{"name":"sam","title":"mr","salary":100.43}
            |{"name":"laura","title":"ms","salary":429.06}
            |{"name":"tom","title":null,"salary":395.44}
            |""".stripMargin.filter(_ >= ' '),
        )
    }

  }

  unitUnderTest should "partition by date" in {

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

    val task = createSinkTask()

    val props = (defaultProps + (
      s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _date.uuuu,_date.LL,_date.dd PROPERTIES('${FlushCount.entryName}'=1,'padding.length.partition'='12', 'padding.length.offset'='12', '${PartitionIncludeKeys.entryName}'=false)",
    )).asJava

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

  unitUnderTest should "write files with topic partition without padding when requested" in {

    val task = createSinkTask()

    val props = (defaultProps ++ Map(
      s"$prefix.kcql"             -> s"insert into $BucketName:$PrefixName select * from $TopicName PROPERTIES('${FlushCount.entryName}'=3)",
      s"$prefix.padding.strategy" -> "NoOp",
      s"$prefix.padding.length"   -> "10",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/1/").size should be(1)

    remoteFileAsString(BucketName, "streamReactorBackups/myTopic/1/2_0_2.json") should be(
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

  private def createHeaders[HX](keyValuePair: (String, HX)*): lang.Iterable[Header] = {
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

}
