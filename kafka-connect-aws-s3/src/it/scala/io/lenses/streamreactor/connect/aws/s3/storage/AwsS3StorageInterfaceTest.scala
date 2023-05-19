package io.lenses.streamreactor.connect.aws.s3.storage

import cats.effect.Clock
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.DefaultConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.InitedConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.PutObjectRequest

class AwsS3StorageInterfaceTest extends AnyFlatSpec with Matchers with S3ProxyContainerTest with LazyLogging {

  private implicit val connectorTaskId: ConnectorTaskId = InitedConnectorTaskId("sinkName", 1, 1)

  override def cleanUpEnabled: Boolean = false

  override def setUpTestData(): Unit = {
    val requestBody = RequestBody.fromString("x")
    Seq("topic-1", "topic-2").foreach {
      topic =>
        for (partitionNo <- 1 to 10) {

          for (offsetNo <- 1 to 2) {
            logger.debug(s"Writing $topic/$partitionNo/$offsetNo")
            s3Client.putObject(
              PutObjectRequest.builder().bucket(BucketName).key(s"$topic/$partitionNo/$offsetNo").build(),
              requestBody,
            )
          }
        }
    }

  }

  "s3StorageInterface" should "list directories within a path" in {

    val s3StorageInterface = new AwsS3StorageInterface()(connectorTaskId, s3Client)

    val topicRoot = RemoteS3RootLocation(BucketName, "topic-1/".some, allowSlash = true)

    val dirs = s3StorageInterface.findDirectories(
      topicRoot,
      DirectoryFindCompletionConfig(0, none, none, Clock[IO]),
      Set.empty,
      Option.empty,
    ).unsafeRunSync()

    val allValues        = (1 to 10).map(x => s"topic-1/$x/")
    val partitionResults = CompletedDirectoryFindResults(allValues.toSet)
    dirs should be(partitionResults)

  }

  "s3StorageInterface" should "list directories within a path recursively from bucket root" in {

    val s3StorageInterface = new AwsS3StorageInterface()(DefaultConnectorTaskId, s3Client)

    val bucketRoot = RemoteS3RootLocation(BucketName, none, allowSlash = true)

    val dirs = s3StorageInterface.findDirectories(
      bucketRoot,
      DirectoryFindCompletionConfig(3, none, none, Clock[IO]),
      Set.empty,
      Option.empty,
    ).unsafeRunSync()

    val allValues  = (1 to 10).map(x => s"topic-1/$x/")
    val allValues2 = (1 to 10).map(x => s"topic-2/$x/")

    val partitionResults = CompletedDirectoryFindResults((allValues ++ allValues2).toSet)
    dirs should be(partitionResults)

  }
}
