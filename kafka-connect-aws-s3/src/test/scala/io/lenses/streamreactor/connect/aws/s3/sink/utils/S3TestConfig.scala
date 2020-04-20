package io.lenses.streamreactor.connect.aws.s3.sink.utils

import java.io._
import java.nio.charset.StandardCharsets
import java.util.Scanner
import java.util.stream.Collectors

import com.google.common.io.ByteStreams
import io.lenses.streamreactor.connect.aws.s3.storage.MultipartBlobStoreStorageInterface
import org.apache.commons.io.FileUtils
import org.jclouds.blobstore.BlobStoreContext
import org.jclouds.io.Payload
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

object S3TestPayloadReader {

  def extractPayload(payload: Payload): String = {
    val state = new Scanner(payload.openStream(), StandardCharsets.UTF_8.name).useDelimiter("\\Z").next
    new BufferedReader(new StringReader(state)).lines().collect(Collectors.joining())
  }

  def toFile(bucketName: String, fileName: String, blobStoreContext: BlobStoreContext, localFileName: String): Unit = {
    val bytes = readPayload(bucketName, fileName, blobStoreContext)
    FileUtils.writeByteArrayToFile(new File(localFileName), bytes)
  }

  def readPayload(bucketName: String, fileName: String, blobStoreContext: BlobStoreContext): Array[Byte] = {
    toByteArray(getPayload(bucketName, fileName, blobStoreContext))
  }

  def getPayload(bucketName: String, fileName: String, blobStoreContext: BlobStoreContext): Payload = {
    blobStoreContext.getBlobStore.getBlob(bucketName, fileName).getPayload
  }

  def toByteArray(payload: Payload): Array[Byte] = {
    ByteStreams.toByteArray(payload.openStream())
  }
}

trait S3TestConfig extends AnyFlatSpec with BeforeAndAfter {

  protected val proxyContext: S3ProxyContext = new S3ProxyContext()

  implicit val blobStoreContext: BlobStoreContext = proxyContext.createBlobStoreContext
  implicit val storageInterface: MultipartBlobStoreStorageInterface = new MultipartBlobStoreStorageInterface(blobStoreContext)

  val BucketName: String = S3ProxyContext.TestBucket

  before {
    proxyContext.startProxy
    proxyContext.createTestBucket
  }

  after {
    proxyContext.stopProxy
  }

  def readFileToString(fileName: String, blobStoreContext: BlobStoreContext): String = {
    try {
      val payload: Payload = S3TestPayloadReader.getPayload(BucketName, fileName, blobStoreContext)
      S3TestPayloadReader.extractPayload(payload)
    } catch {
      case t: Throwable => fail("Unable to read file", t)
    }

  }

}
