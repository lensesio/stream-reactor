package io.lenses.streamreactor.connect.aws.s3.utils

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.testcontainers.containers.GenericContainer

trait CloudPlatformEmulatorSuite[SM <: FileMetadata, SI <: StorageInterface[SM], C]
    extends AnyFlatSpec
    with BeforeAndAfter
    with BeforeAndAfterAll
    with RemoteFileHelper[SI] {
  case class CloudPlatformState(client: C, s3StorageInterface: SI, defaultProps: Map[String, String])

  val container: GenericContainer[_]

  var state: Option[CloudPlatformState] = None

  def client: C = state.map(_.client).getOrElse(fail("Unset client"))

  implicit def storageInterface: SI = state.map(_.s3StorageInterface).getOrElse(fail("Unset SI"))

  def defaultProps: Map[String, String] = state.map(_.defaultProps).getOrElse(fail("Unset props"))

  def createClient(): C
  def createStorageInterface(client: C): SI
  def createDefaultProps(): Map[String, String]

  def createBucket(): Unit

  override protected def beforeAll(): Unit = {
    container.start()

    val client = createClient()
    state = CloudPlatformState(
      client,
      createStorageInterface(client),
      createDefaultProps(),
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
