package io.lenses.streamreactor.connect.aws.s3.utils

import com.dimafeng.testcontainers.ForAllTestContainer
import com.dimafeng.testcontainers.GenericContainer
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.auth.AwsS3ClientCreator
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.S3Config
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.testcontainers.containers.wait.strategy.Wait
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.Delete
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.ObjectIdentifier

import java.io.File
import java.nio.file.Files
import scala.util.Try

trait S3ProxyContainerTest extends AnyFlatSpec with ForAllTestContainer with LazyLogging with BeforeAndAfter {
  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("unit-tests", 1, 1)
  val Port:                             Int             = 8080
  val Identity:                         String          = "identity"
  val Credential:                       String          = "credential"
  val BucketName:                       String          = "employees"

  var storageInterfaceOpt: Option[AwsS3StorageInterface] = None
  var s3ClientOpt:         Option[S3Client]              = None
  var helperOpt:           Option[RemoteFileHelper]      = None

  var localRoot: File = _
  var localFile: File = _

  implicit lazy val storageInterface: AwsS3StorageInterface =
    storageInterfaceOpt.getOrElse(throw new IllegalStateException("Test not initialised"))

  lazy val s3Client: S3Client         = s3ClientOpt.getOrElse(throw new IllegalStateException("Test not initialised"))
  lazy val helper:   RemoteFileHelper = helperOpt.getOrElse(throw new IllegalStateException("Test not initialised"))

  override val container: GenericContainer = GenericContainer(
    dockerImage  = "andrewgaul/s3proxy:sha-ba0fd6d",
    exposedPorts = Seq(Port),
    env = Map[String, String](
      "S3PROXY_ENDPOINT" -> ("http://0.0.0.0:" + Port),
      // S3Proxy currently has an issue with authorization, therefore it is disabled for the time being
      // https://github.com/gaul/s3proxy/issues/392
      "S3PROXY_AUTHORIZATION" -> "none",
      "S3PROXY_IDENTITY"      -> Identity,
      "S3PROXY_CREDENTIAL"    -> Credential,
      // using the AWS library requires this to be set for testing
      "S3PROXY_IGNORE_UNKNOWN_HEADERS" -> "true",
    ),
    waitStrategy = Wait.forListeningPort(),
  )

  def uri(): String = "http://127.0.0.1:" + container.mappedPort(Port)

  def resume(): Unit = {
    val _ = container.dockerClient.unpauseContainerCmd(container.containerId).exec();
  }

  def pause(): Unit = {
    val _ = container.dockerClient.pauseContainerCmd(container.containerId).exec()
  }

  override def afterStart(): Unit = {

    val (sI, sC) = {
      for {
        client           <- AwsS3ClientCreator.make(s3Config)
        storageInterface <- Try(new AwsS3StorageInterface(connectorTaskId, client, true)).toEither
      } yield (storageInterface, client)
    }.getOrElse(fail("Failed to create S3 client"))

    storageInterfaceOpt = Some(sI)
    s3ClientOpt         = Some(sC)
    helperOpt           = Some(new RemoteFileHelper(sI))

    logger.debug("Creating test bucket")
    createTestBucket(BucketName) match {
      case Left(err) =>
        logger.error("Failed to create test bucket.", err)
        fail("Failed to create test bucket.", err)
      case Right(_) =>
    }
    setUpTestData()

    localRoot = Files.createTempDirectory("blah").toFile
    localFile = Files.createTempFile("blah", "blah").toFile
  }

  def cleanUpEnabled: Boolean = true

  def setUpTestData(): Unit = {}

  def s3Config: S3Config = S3Config(
    region                   = Some("us-east-1"),
    accessKey                = Some(Identity),
    secretKey                = Some(Credential),
    authMode                 = AuthMode.Credentials,
    customEndpoint           = Some(uri()),
    enableVirtualHostBuckets = true,
  )

  after {
    if (cleanUpEnabled) {
      clearTestBucket(BucketName)
      setUpTestData()
    }
  }

  def createTestBucket(bucketName: String): Either[Throwable, Unit] =
    // It is fine if it already exists
    Try(s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())).toEither.map(_ => ())

  def clearTestBucket(bucketName: String): Either[Throwable, Unit] =
    Try {

      val toDeleteArray = helper
        .listBucketPath(bucketName, "")
        .map(ObjectIdentifier.builder().key(_).build())
      val delete = Delete.builder().objects(toDeleteArray: _*).build
      s3Client.deleteObjects(DeleteObjectsRequest.builder().bucket(BucketName).delete(delete).build())

    }.toEither.map(_ => ())

}
