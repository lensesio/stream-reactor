package io.lenses.streamreactor.connect.aws.s3.storage

import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.storage.DirectoryFindCompletionConfig
import io.lenses.streamreactor.connect.cloud.common.storage.DirectoryFindResults
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import scala.jdk.CollectionConverters.IteratorHasAsScala

class ListDirectoryTest extends AnyFlatSpec with Matchers with S3ProxyContainerTest with LazyLogging {

  implicit val cloudLocationValidator: CloudLocationValidator = S3LocationValidator

  override def cleanUp(): Unit = ()

  override def setUpTestData(): Either[Throwable, Unit] = {
    val requestBody = RequestBody.fromString("x")
    Seq("topic-1", "topic-2").foreach {
      topic =>
        for (partitionNo <- 1 to 10) {

          for (offsetNo <- 1 to 2) {
            logger.debug(s"Writing $topic/$partitionNo/$offsetNo")
            client.putObject(
              PutObjectRequest.builder().bucket(BucketName).key(s"$topic/$partitionNo/$offsetNo").build(),
              requestBody,
            )
          }
        }
    }
    ().asRight
  }

  "s3StorageInterface" should "list directories within a path" in {

    val connectorTaskId: ConnectorTaskId = ConnectorTaskId("sinkName", 1, 1)

    val topicRoot = CloudLocation(BucketName, "topic-1/".some)

    val dirs = AwsS3DirectoryLister.findDirectories(
      topicRoot,
      DirectoryFindCompletionConfig(0),
      Set.empty,
      Set.empty,
      client.listObjectsV2Paginator(_).iterator().asScala,
      connectorTaskId,
    ).unsafeRunSync()

    val allValues        = (1 to 10).map(x => s"topic-1/$x/")
    val partitionResults = DirectoryFindResults(allValues.toSet)
    dirs should be(partitionResults)

  }

  "s3StorageInterface" should "return empty on directories within a path 2 levels deep from bucket root" in {

    val taskId = ConnectorTaskId("sinkName", 1, 0)

    val bucketRoot = CloudLocation(BucketName)

    val dirs = AwsS3DirectoryLister.findDirectories(
      bucketRoot,
      DirectoryFindCompletionConfig(2),
      Set.empty,
      Set.empty,
      client.listObjectsV2Paginator(_).iterator().asScala,
      taskId,
    ).unsafeRunSync()

    val partitionResults = DirectoryFindResults(Set.empty)
    dirs should be(partitionResults)

  }

  "s3StorageInterface" should "return empty on listing directories within a path 3 levels deep from bucket root" in {

    val taskId = ConnectorTaskId("sinkName", 1, 0)

    val bucketRoot = CloudLocation(BucketName)

    val dirs = AwsS3DirectoryLister.findDirectories(
      bucketRoot,
      DirectoryFindCompletionConfig(3),
      Set.empty,
      Set.empty,
      client.listObjectsV2Paginator(_).iterator().asScala,
      taskId,
    ).unsafeRunSync()

    val partitionResults = DirectoryFindResults(Set.empty)
    dirs should be(partitionResults)

  }
  "s3StorageInterface" should "list directories within a path 1 levels deep from bucket root" in {

    val taskId = ConnectorTaskId("sinkName", 1, 0)

    val bucketRoot = CloudLocation(BucketName)

    val dirs = AwsS3DirectoryLister.findDirectories(
      bucketRoot,
      DirectoryFindCompletionConfig(1),
      Set.empty,
      Set.empty,
      client.listObjectsV2Paginator(_).iterator().asScala,
      taskId,
    ).unsafeRunSync()

    val allValues = (1 to 10).flatMap(x => List(s"topic-1/$x/", s"topic-2/$x/"))

    val partitionResults = DirectoryFindResults(allValues.toSet)
    dirs should be(partitionResults)

  }
}
