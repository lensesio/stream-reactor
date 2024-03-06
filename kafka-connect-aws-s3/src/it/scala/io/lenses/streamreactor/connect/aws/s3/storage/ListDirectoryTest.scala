package io.lenses.streamreactor.connect.aws.s3.storage

import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.utils.AsyncIOAssertions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ListDirectoryTest
    extends AnyFlatSpec
    with Matchers
    with S3ProxyContainerTest
    with AsyncIOAssertions
    with LazyLogging {

  implicit val cloudLocationValidator: CloudLocationValidator = S3LocationValidator

  override def cleanUp(): Unit = ()

  override def setUpTestData(storageInterface: AwsS3StorageInterface): Either[Throwable, Unit] = {
    Seq("topic-1", "topic-2").foreach {
      topic =>
        for (partitionNo <- 1 to 10) {

          for (offsetNo <- 1 to 2) {
            logger.debug(s"Writing $topic/$partitionNo/$offsetNo")
            storageInterface.writeStringToFile(BucketName, s"$topic/$partitionNo/$offsetNo", UploadableString("x"))
          }
        }
    }
    ().asRight
  }

  "s3StorageInterface" should "list directories within a path" in {

    val connectorTaskId: ConnectorTaskId = ConnectorTaskId("sinkName", 1, 1)

    val topicRoot = CloudLocation(BucketName, "topic-1/".some)

    new AwsS3DirectoryLister(connectorTaskId, client).findDirectories(
      topicRoot,
      0,
      Set.empty,
      Set.empty,
    ).asserting {
      dirs =>
        val allValues        = (1 to 10).map(x => s"topic-1/$x/")
        val partitionResults = allValues.toSet
        dirs should be(partitionResults)
        ()
    }

  }

  "s3StorageInterface" should "return empty on directories within a path 2 levels deep from bucket root" in {

    val taskId = ConnectorTaskId("sinkName", 1, 0)

    val bucketRoot = CloudLocation(BucketName)

    new AwsS3DirectoryLister(taskId, client).findDirectories(bucketRoot, 2, Set.empty, Set.empty).asserting {
      dirs =>
        val partitionResults = Set.empty
        dirs should be(partitionResults)
        ()
    }

  }

  "s3StorageInterface" should "return empty on listing directories within a path 3 levels deep from bucket root" in {

    val taskId = ConnectorTaskId("sinkName", 1, 0)

    val bucketRoot = CloudLocation(BucketName)

    new AwsS3DirectoryLister(taskId, client).findDirectories(bucketRoot, 3, Set.empty, Set.empty).asserting {
      dirs =>
        val partitionResults = Set.empty
        dirs should be(partitionResults)
        ()
    }

  }

  "s3StorageInterface" should "list directories within a path 1 levels deep from bucket root" in {

    val taskId = ConnectorTaskId("sinkName", 1, 0)

    val bucketRoot = CloudLocation(BucketName)

    new AwsS3DirectoryLister(taskId, client).findDirectories(bucketRoot, 1, Set.empty, Set.empty).asserting {
      dirs =>
        val allValues = (1 to 10).flatMap(x => List(s"topic-1/$x/", s"topic-2/$x/"))

        val partitionResults = allValues.toSet
        dirs should be(partitionResults)
        ()
    }

  }

}
