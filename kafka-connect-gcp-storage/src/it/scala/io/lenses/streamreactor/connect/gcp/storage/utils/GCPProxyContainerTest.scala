package io.lenses.streamreactor.connect.gcp.storage.utils

import cats.implicits.catsSyntaxApplicativeError
import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.Storage
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.TaskIndexKey
import io.lenses.streamreactor.connect.cloud.common.sink.CloudPlatformEmulatorSuite
import io.lenses.streamreactor.connect.gcp.storage.auth.GCPStorageClientCreator
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings._
import io.lenses.streamreactor.connect.gcp.storage.config.AuthMode
import io.lenses.streamreactor.connect.gcp.storage.config.AuthModeSettingsConfigKeys
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConnectionConfig
import io.lenses.streamreactor.connect.gcp.storage.config.UploadConfigKeys
import io.lenses.streamreactor.connect.gcp.storage.sink.GCPStorageSinkTask
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageFileMetadata
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageStorageInterface
import io.lenses.streamreactor.connect.testcontainers.GCPStorageContainer
import io.lenses.streamreactor.connect.testcontainers.PausableContainer
import io.lenses.streamreactor.connect.testcontainers.TestContainersPausableContainer

import java.io.File
import java.nio.file.Files
import scala.util.Try

trait GCPProxyContainerTest
    extends CloudPlatformEmulatorSuite[GCPStorageFileMetadata, GCPStorageStorageInterface, GCPStorageSinkTask, Storage]
    with TaskIndexKey
    with AuthModeSettingsConfigKeys
    with UploadConfigKeys
    with LazyLogging {

  implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("unit-tests", 1, 1)

  override val container: PausableContainer = new TestContainersPausableContainer(GCPStorageContainer())

  override val prefix: String = "connect.gcpstorage"

  override def createStorageInterface(client: Storage): Either[Throwable, GCPStorageStorageInterface] =
    Try(new GCPStorageStorageInterface(connectorTaskId, client, true)).toEither

  override def createClient(): Either[Throwable, Storage] = {

    val gcpConfig: GCPConnectionConfig = GCPConnectionConfig(
      projectId      = Some("test"),
      quotaProjectId = Option.empty,
      authMode       = AuthMode.None,
      host           = Some(container.getEndpointUrl()),
    )

    GCPStorageClientCreator.make(gcpConfig)
  }

  override def createSinkTask() = new GCPStorageSinkTask()

  lazy val defaultProps: Map[String, String] =
    Map(
      GCP_PROJECT_ID         -> "projectId",
      AUTH_MODE              -> "none",
      HOST                   -> container.getEndpointUrl(),
      "name"                 -> "gcpSinkTaskTest",
      TASK_INDEX             -> "1:1",
      AVOID_RESUMABLE_UPLOAD -> "true",
    )

  val localRoot: File = Files.createTempDirectory("blah").toFile
  val localFile: File = Files.createTempFile("blah", "blah").toFile

  override def connectorPrefix: String = CONNECTOR_PREFIX

  override def createBucket(client: Storage): Either[Throwable, Unit] =
    Try {
      client.create(BucketInfo.of(BucketName))
      ()
    }.toEither

  override def cleanUp(): Unit = {
    for {
      toDeleteArray <- listBucketPathEither(BucketName, "")
      _ <- storageInterface.deleteFiles(BucketName, toDeleteArray)
        .recover(err => logger.error(" _", err))
    } yield ()
    ()
  }

}
