package io.lenses.streamreactor.connect.aws.s3.utils
import cats.implicits.toBifunctorOps
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.auth.AwsS3ClientCreator
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.S3Config
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.AUTH_MODE
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.AWS_ACCESS_KEY
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.AWS_REGION
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.AWS_SECRET_KEY
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.CONNECTOR_PREFIX
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.CUSTOM_ENDPOINT
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.ENABLE_VIRTUAL_HOST_BUCKETS
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.aws.s3.storage.S3FileMetadata
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.TaskIndexKey
import io.lenses.streamreactor.connect.testcontainers.S3Container
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.Delete
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.ObjectIdentifier

import java.io.File
import java.nio.file.Files
import scala.util.Try

trait S3ProxyContainerTest
    extends CloudPlatformEmulatorSuite[S3FileMetadata, AwsS3StorageInterface, S3Client]
    with TaskIndexKey
    with LazyLogging {

  val BucketName = "testbucket"

  implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("unit-tests", 1, 1)

  override val container: S3Container = S3Container()

  override def createStorageInterface(client: S3Client): AwsS3StorageInterface =
    Try(new AwsS3StorageInterface(connectorTaskId, client, true)).toEither.leftMap(fail(_)).merge

  override def createClient(): S3Client = {

    val uri: String = "http://127.0.0.1:" + container.getFirstMappedPort

    val s3Config: S3Config = S3Config(
      region                   = Some("eu-west-1"),
      accessKey                = Some(container.identity.identity),
      secretKey                = Some(container.identity.credential),
      authMode                 = AuthMode.Credentials,
      customEndpoint           = Some(uri),
      enableVirtualHostBuckets = true,
    )

    AwsS3ClientCreator.make(s3Config).leftMap(fail(_)).merge
  }

  override def createDefaultProps(): Map[String, String] = {

    val uri: String = "http://127.0.0.1:" + container.getFirstMappedPort

    Map(
      AWS_ACCESS_KEY              -> container.identity.identity,
      AWS_SECRET_KEY              -> container.identity.credential,
      AUTH_MODE                   -> AuthMode.Credentials.toString,
      CUSTOM_ENDPOINT             -> uri,
      ENABLE_VIRTUAL_HOST_BUCKETS -> "true",
      "name"                      -> "s3SinkTaskBuildLocalTest",
      AWS_REGION                  -> "eu-west-1",
      TASK_INDEX                  -> "1:1",
    )
  }

  val localRoot: File = Files.createTempDirectory("blah").toFile
  val localFile: File = Files.createTempFile("blah", "blah").toFile

  override def connectorPrefix: String = CONNECTOR_PREFIX

  override def createBucket(): Unit = {
    Try(
      client.createBucket(CreateBucketRequest.builder().bucket(BucketName).build()),
    ).toEither.leftMap(logger.error(" _", _))
    ()
  }

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
