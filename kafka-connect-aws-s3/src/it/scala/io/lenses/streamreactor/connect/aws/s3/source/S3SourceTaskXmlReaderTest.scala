package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceSettingsKeys
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Try
import scala.xml.XML

class S3SourceTaskXmlReaderTest
    extends S3ProxyContainerTest
    with AnyFlatSpecLike
    with Matchers
    with EitherValues
    with CloudSourceSettingsKeys {

  def DefaultProps: Map[String, String] = defaultProps + (
    SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> "1000",
  )

  val PrefixName = "streamReactorBackups"
  val TopicName  = "myTopic"

  override def cleanUp(): Unit = ()

  override def setUpTestData(storageInterface: AwsS3StorageInterface): Either[Throwable, Unit] = {
    val res = Seq("/xml/employeedata0001.xml", "/xml/employeedata0002.xml").traverse { f =>
      val file = new File(getClass.getResource(f).toURI)
      storageInterface.uploadFile(UploadableFile(file), BucketName, "streamReactorBackups" + f)
    }
    res.value.foreach(_ should not be empty)
    ().asRight
  }

  "task" should "extract from xml files" in {

    val task = new S3SourceTask()

    val props = (defaultProps ++ Map(
      "connect.s3.kcql"                                   -> s"insert into $TopicName select * from $BucketName:$PrefixName/xml STOREAS `TEXT` LIMIT 1000 PROPERTIES ( 'read.text.mode'='StartEndTag' , 'read.text.start.tag'='<Employee>' , 'read.text.end.tag'='</Employee>' )",
      "connect.s3.source.partition.search.recurse.levels" -> "0",
    )).asJava

    task.start(props)

    try {
      val sourceRecords      = SourceRecordsLoop.loop(task, 10.seconds.toMillis, 20000).value
      val sourceRecordsMopUp = task.poll()
      sourceRecordsMopUp should be(empty)

      val firstEmployee = Employee.fromSourceRecord(sourceRecords.head)
      firstEmployee.value should be(
        Employee(
          "1",
          "3-1991",
          "B1",
          "Fairly Paid",
          "Skydiving Instructor Extraordinaire",
        ),
      )

      val lastEmployee = Employee.fromSourceRecord(sourceRecords.last)
      lastEmployee.value should be(
        Employee(
          "20,000",
          "1-2011",
          "C1",
          "Massively Underpaid",
          "Skydiving Instructor for Schools",
        ),
      )
    } finally {
      task.stop()
    }

  }

  case class Employee(number: String, startMonth: String, bracket: String, bracketDescription: String, category: String)

  object Employee {

    def fromSourceRecord(sourceRecord: SourceRecord): Either[Throwable, Employee] =
      sourceRecord.value() match {
        case empXml: String =>
          Try {
            val xml = XML.loadString(empXml)
            Employee(
              (xml \ "Number").text,
              (xml \ "StartMonth").text,
              (xml \ "Bracket").text,
              (xml \ "BracketDescription").text,
              (xml \ "Category").text,
            )
          }.toEither
      }
  }

}
