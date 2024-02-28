package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceSettingsKeys
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.EitherValues
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

class S3SourceJsonEnvelopeTest
    extends S3ProxyContainerTest
    with AnyFlatSpecLike
    with Matchers
    with EitherValues
    with CloudSourceSettingsKeys {

  def DefaultProps: Map[String, String] = defaultProps + (
    SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> "1000",
  )

  val MyPrefix  = "backups"
  val TopicName = "myTopic"

  override def cleanUp(): Unit = ()

  override def setUpTestData(storageInterface: AwsS3StorageInterface): Either[Throwable, Unit] = {
    val envelopeJson =
      """{"key":{"id":"1"},"value":{"id":"1","name":"John Smith","email":"js@johnsmith.com","card":"1234567890","ip":"192.168.0.2","country":"UK","currency":"GBP","timestamp":"2020-01-01T00:00:00.000Z"},"headers":{"header1":"value1","header2":123456789},"metadata":{"timestamp":1234567890,"topic":"myTopic","partition":3,"offset":0}}""".stripMargin
    val file = new java.io.File("00001.json")
    try {
      //write the json to file as text
      val bw = new java.io.BufferedWriter(new java.io.FileWriter(file))
      bw.write(envelopeJson)
      bw.flush()
      bw.close()
      storageInterface.uploadFile(UploadableFile(file), BucketName, s"$MyPrefix/json/0")
      ().asRight
    } finally {
      file.delete()
      ()
    }
  }

  "task" should "extract from json files containing the envelope" in {

    val task = new S3SourceTask()

    val props = (defaultProps ++ Map(
      "connect.s3.kcql"                            -> s"insert into $TopicName select * from $BucketName:$MyPrefix/json STOREAS `JSON` LIMIT 1000 PROPERTIES ('store.envelope'=true)",
      "connect.s3.partition.search.recurse.levels" -> "0",
      "connect.partition.search.continuous"        -> "false",
    )).asJava

    task.start(props)

    var sourceRecords: Seq[SourceRecord] = List.empty
    try {
      eventually {
        do {
          sourceRecords = sourceRecords ++ task.poll().asScala
        } while (sourceRecords.size != 1)
        task.poll() should be(empty)
      }
    } finally {
      task.stop()
    }

    //assert the record matches the envelope
    val sourceRecord = sourceRecords.head
    sourceRecord.key() should be("""{"id":"1"}""")

    sourceRecord.value() shouldBe """{"id":"1","name":"John Smith","email":"js@johnsmith.com","card":"1234567890","ip":"192.168.0.2","country":"UK","currency":"GBP","timestamp":"2020-01-01T00:00:00.000Z"}"""

    sourceRecord.headers().asScala.map(h => h.key() -> h.value()).toMap should be(Map("header1" -> "value1",
                                                                                      "header2" -> 123456789L,
    ))

    sourceRecord.sourcePartition().asScala shouldBe Map("container" -> BucketName, "prefix" -> s"$MyPrefix/json/")
    val sourceOffsetMap = sourceRecord.sourceOffset().asScala
    sourceOffsetMap("path") shouldBe s"$MyPrefix/json/0"
    sourceOffsetMap("line") shouldBe "0"
    sourceOffsetMap("ts").toString.toLong < Instant.now().toEpochMilli shouldBe true

    sourceRecord.topic() shouldBe TopicName
    sourceRecord.kafkaPartition() shouldBe 3
    sourceRecord.timestamp() shouldBe 1234567890L
  }

}
