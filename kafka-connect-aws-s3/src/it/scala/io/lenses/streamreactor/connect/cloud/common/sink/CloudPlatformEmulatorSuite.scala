package io.lenses.streamreactor.connect.cloud.common.sink

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.utils.RemoteFileHelper
import io.lenses.streamreactor.connect.testcontainers.PausableContainer
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

trait CloudPlatformEmulatorSuite[SM <: FileMetadata, SI <: StorageInterface[SM], T <: CloudSinkTask[SM], C]
    extends AnyFlatSpec
    with BeforeAndAfter
    with BeforeAndAfterAll
    with RemoteFileHelper[SI] {

  def createSinkTask(): T

  val prefix: String
  val BucketName = "testbucket"

  case class CloudPlatformState(client: C, s3StorageInterface: SI)

  val container: PausableContainer

  var state: Option[CloudPlatformState] = None

  def client: C = state.map(_.client).getOrElse(fail("Unset client"))

  implicit def storageInterface: SI = state.map(_.s3StorageInterface).getOrElse(fail("Unset SI"))

  def createClient(): C
  def createStorageInterface(client: C): SI
  val defaultProps: Map[String, String]

  def createBucket(): Unit

  override protected def beforeAll(): Unit = {
    container.start()

    val client = createClient()
    state = CloudPlatformState(
      client,
      createStorageInterface(client),
    ).some
    createBucket()
    setUpTestData()
  }

  override protected def afterAll(): Unit =
    container.stop()

  after {
    cleanUp()
  }

  def setUpTestData(): Unit = ()

  def cleanUp(): Unit = ()

}
