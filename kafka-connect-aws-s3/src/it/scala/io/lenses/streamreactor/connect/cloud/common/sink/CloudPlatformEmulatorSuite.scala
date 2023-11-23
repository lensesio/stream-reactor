package io.lenses.streamreactor.connect.cloud.common.sink

import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import cats.implicits.toBifunctorOps
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.utils.RemoteFileHelper
import io.lenses.streamreactor.connect.testcontainers.PausableContainer
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.Try

trait CloudPlatformEmulatorSuite[SM <: FileMetadata, SI <: StorageInterface[SM], T <: CloudSinkTask[SM], C]
    extends AnyFlatSpec
    with BeforeAndAfter
    with BeforeAndAfterAll
    with RemoteFileHelper[SI] {

  def createSinkTask(): T

  val prefix: String
  val BucketName = "testbucket"

  val container: PausableContainer

  var maybeClient:           Option[C]  = None
  var maybeStorageInterface: Option[SI] = None

  def client: C = maybeClient.getOrElse(fail("Unset client"))

  implicit def storageInterface: SI = maybeStorageInterface.getOrElse(fail("Unset SI"))

  def createClient(): Either[Throwable, C]
  def createStorageInterface(client: C): Either[Throwable, SI]

  val defaultProps: Map[String, String]

  def createBucket(): Either[Throwable, Unit]

  override protected def beforeAll(): Unit = {

    {
      for {
        _      <- Try(container.start()).toEither
        client <- createClient()
        sI     <- createStorageInterface(client)
        _      <- createBucket()
        _      <- setUpTestData()
      } yield {
        maybeClient           = client.some
        maybeStorageInterface = sI.some
      }
    }.leftMap(fail(_))
    ()
  }

  override protected def afterAll(): Unit =
    container.stop()

  after {
    cleanUp()
  }

  def setUpTestData(): Either[Throwable, Unit] = ().asRight

  def cleanUp(): Unit = ()

}
