package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsJava}

class S3SourceTaskXmlReaderTest
    extends S3ProxyContainerTest
    with AnyFlatSpecLike
    with Matchers {

  def DefaultProps: Map[String, String] = Map(
    AWS_ACCESS_KEY -> Identity,
    AWS_SECRET_KEY -> Credential,
    AWS_REGION -> "eu-west-1",
    AUTH_MODE -> AuthMode.Credentials.toString,
    CUSTOM_ENDPOINT -> uri(),
    ENABLE_VIRTUAL_HOST_BUCKETS -> "true",
    TASK_INDEX -> "1:1",
    "name" -> "s3-source",
    SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> "1000",
  )

  val PrefixName = "streamReactorBackups"
  val TopicName = "myTopic"

  override def cleanUpEnabled: Boolean = false

  override def setUpTestData(): Unit = {
    val res = Seq("/xml/employeedata0001.xml", "/xml/employeedata0002.xml").traverse { f =>
      val file = new File(getClass.getResource(f).toURI)
      storageInterface.uploadFile(file, BucketName, "streamReactorBackups" + f)
    }
    res should be(Right(Vector((), ())))
    ()
  }

  "task" should "extract from xml files" in {

    val task = new S3SourceTask()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into ${TopicName} select * from $BucketName:${PrefixName}/xml STOREAS `TEXT` LIMIT 1000 PROPERTIES ( 'read.text.mode'='StartEndTag' , 'read.text.start.tag'='<Employee>' , 'read.text.end.tag'='</Employee>' )",
          "connect.s3.partition.search.recurse.levels" -> "0"
        ),
      ).asJava

    task.start(props)

    var sourceRecords : Seq[SourceRecord] = List.empty
    eventually {
      do {
        sourceRecords = sourceRecords ++ task.poll().asScala
      } while (sourceRecords.size != 20000)
      val sourceRecordsMopUp = task.poll()
      sourceRecordsMopUp should be(empty)
    }

    task.stop()

  }

}
