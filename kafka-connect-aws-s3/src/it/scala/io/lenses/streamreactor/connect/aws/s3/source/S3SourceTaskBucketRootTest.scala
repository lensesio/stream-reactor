package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.source.S3SourceTaskTest.formats
import io.lenses.streamreactor.connect.aws.s3.source.config.SourcePartitionSearcherSettingsKeys
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import org.scalatest.EitherValues
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import software.amazon.awssdk.services.s3.model.CreateBucketRequest

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Try
class S3SourceTaskBucketRootTest
    extends S3ProxyContainerTest
    with AnyFlatSpecLike
    with Matchers
    with EitherValues
    with SourcePartitionSearcherSettingsKeys {

  def DefaultProps: Map[String, String] = defaultProps + (
    SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> "1000",
  )

  private val TopicName = "myTopic"

  override def cleanUp(): Unit = ()

  "task" should "read files from root of bucket" in {
    forAll(formats) {
      (format, formatExtension, _) =>
        withClue(s"Format:$format") {
          val bucketSetup = new BucketSetup()(storageInterface)
          val bucketName  = (BucketName + format.entryName + formatExtension.map(_.entryName).getOrElse("")).toLowerCase
          createBucket(bucketName) should be(Right(()))
          bucketSetup.setUpRootBucketData(
            bucketName,
            format,
            formatExtension,
          )
          val task = new S3SourceTask()

          val props = DefaultProps
            .combine(
              Map(
                KCQL_CONFIG -> s"insert into $TopicName select * from $bucketName STOREAS `${format.entryName}${formatExtension.fold("")("_" + _)}` LIMIT 190",
              ),
            ).asJava

          task.start(props)

          withCleanup(task.stop()) {
            val sourceRecords1 = eventually {
              val records = task.poll()
              records.size() shouldBe 190
              records
            }

            val sourceRecords2 = task.poll()
            val sourceRecords3 = task.poll()
            val sourceRecords4 = task.poll()
            val sourceRecords5 = task.poll()
            val sourceRecords6 = task.poll()
            val sourceRecords7 = task.poll()

            task.stop()

            sourceRecords2 should have size 190
            sourceRecords3 should have size 190
            sourceRecords4 should have size 190
            sourceRecords5 should have size 190
            sourceRecords6 should have size 50
            sourceRecords7 should have size 0

            sourceRecords1.asScala
              .concat(sourceRecords2.asScala)
              .concat(sourceRecords3.asScala)
              .concat(sourceRecords4.asScala)
              .concat(sourceRecords5.asScala)
              .concat(sourceRecords6.asScala)
              .toSet should have size 1000
          }
        }
    }

    def withCleanup[T](cleanup: => Unit)(fn: => T): Unit =
      try {
        fn
        ()
      } finally {
        cleanup
      }
  }

  private def createBucket(bucketName: String) =
    Try(client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())).toEither.map(_ => ())

}
