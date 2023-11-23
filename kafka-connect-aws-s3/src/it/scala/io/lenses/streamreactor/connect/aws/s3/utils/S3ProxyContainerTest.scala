package io.lenses.streamreactor.connect.aws.s3.utils
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.auth.AwsS3ClientCreator
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.S3Config
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.sink.S3SinkTask
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.aws.s3.storage.S3FileMetadata
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.TaskIndexKey
import io.lenses.streamreactor.connect.cloud.common.sink.CloudPlatformEmulatorSuite
import io.lenses.streamreactor.connect.testcontainers.PausableContainer
import io.lenses.streamreactor.connect.testcontainers.S3Container
import io.lenses.streamreactor.connect.testcontainers.TestContainersPausableContainer
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.Delete
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.ObjectIdentifier

import java.io.File
import java.nio.file.Files
import scala.util.Try

trait S3ProxyContainerTest
    extends CloudPlatformEmulatorSuite[S3FileMetadata, AwsS3StorageInterface, S3SinkTask, S3Client]
    with TaskIndexKey
    with LazyLogging {

  implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("unit-tests", 1, 1)

  val s3Container = S3Container()
  override val container: PausableContainer = new TestContainersPausableContainer(s3Container)

  override val prefix: String = "connect.s3"

  override def createStorageInterface(client: S3Client): Either[Throwable, AwsS3StorageInterface] =
    Try(new AwsS3StorageInterface(connectorTaskId, client, true)).toEither

  override def createClient(): Either[Throwable, S3Client] = {

    val s3Config: S3Config = S3Config(
      region                   = Some("eu-west-1"),
      accessKey                = Some(s3Container.identity.identity),
      secretKey                = Some(s3Container.identity.credential),
      authMode                 = AuthMode.Credentials,
      customEndpoint           = Some(s3Container.getEndpointUrl.toString),
      enableVirtualHostBuckets = true,
    )

    AwsS3ClientCreator.make(s3Config)
  }

  override def createSinkTask() = new S3SinkTask()

  lazy val defaultProps: Map[String, String] =
    Map(
      AWS_ACCESS_KEY              -> s3Container.identity.identity,
      AWS_SECRET_KEY              -> s3Container.identity.credential,
      AUTH_MODE                   -> AuthMode.Credentials.toString,
      CUSTOM_ENDPOINT             -> s3Container.getEndpointUrl.toString,
      ENABLE_VIRTUAL_HOST_BUCKETS -> "true",
      "name"                      -> "s3SinkTaskBuildLocalTest",
      AWS_REGION                  -> "eu-west-1",
      TASK_INDEX                  -> "1:1",
    )

  val localRoot: File = Files.createTempDirectory("blah").toFile
  val localFile: File = Files.createTempFile("blah", "blah").toFile

  override def connectorPrefix: String = CONNECTOR_PREFIX

  override def createBucket(client: S3Client): Either[Throwable, Unit] =
    Try {
      client.createBucket(CreateBucketRequest.builder().bucket(BucketName).build())
      ()
    }.toEither

  override def cleanUp(): Unit = {
    Try {
      val toDeleteArray = listBucketPath(BucketName, "")
        .map(ObjectIdentifier.builder().key(_).build())
      val delete = Delete.builder().objects(toDeleteArray: _*).build
      client.deleteObjects(DeleteObjectsRequest.builder().bucket(BucketName).delete(delete).build())
    }
    ()
  }

}
