package io.lenses.streamreactor.connect.cloud.common.sink

import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import cats.implicits.toBifunctorOps
import io.lenses.streamreactor.common.config.base.intf.ConnectionConfig
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSinkConfig
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.utils.RemoteFileHelper
import io.lenses.streamreactor.connect.testcontainers.PausableContainer
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Try

trait CloudPlatformEmulatorSuite[
  MD <: FileMetadata,
  SI <: StorageInterface[MD],
  C <: CloudSinkConfig[CC],
  CC <: ConnectionConfig,
  CT,
  T <: CloudSinkTask[MD, C, CC, CT],
] extends AnyFlatSpec
    with BeforeAndAfter
    with BeforeAndAfterAll
    with RemoteFileHelper[SI] {

  def createSinkTask(): T

  val prefix: String
  val BucketName = "testbucket"

  val container: PausableContainer

  var maybeStorageInterface: Option[SI] = None

  var maybeClient: Option[CT] = None

  implicit def storageInterface: SI = maybeStorageInterface.getOrElse(fail("Unset SI"))
  def client:                    CT = maybeClient.getOrElse(fail("Unset client"))

  def createClient(): Either[Throwable, CT]
  def createStorageInterface(client: CT): Either[Throwable, SI]

  val defaultProps: Map[String, String]

  def createBucket(client: CT): Either[Throwable, Unit]

  override protected def beforeAll(): Unit = {

    {
      for {
        _      <- Try(container.start()).toEither
        client <- createClient()
        sI     <- createStorageInterface(client)
        _      <- createBucket(client)
        _      <- setUpTestData(sI)
      } yield {
        maybeStorageInterface = sI.some
        maybeClient           = client.some
      }
    }.leftMap(fail(_))
    ()
  }

  override protected def afterAll(): Unit =
    container.stop()

  after {
    cleanUp()
  }

  def setUpTestData(storageInterface: SI): Either[Throwable, Unit] = ().asRight

  def cleanUp(): Unit = ()

}
