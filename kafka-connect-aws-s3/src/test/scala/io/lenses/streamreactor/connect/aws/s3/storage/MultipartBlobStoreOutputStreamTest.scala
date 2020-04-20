package io.lenses.streamreactor.connect.aws.s3.storage

import io.lenses.streamreactor.connect.aws.s3.BucketAndPath
import org.jclouds.blobstore.domain.{MultipartPart, MultipartUpload}
import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, ArgumentMatchers, MockitoSugar}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class MultipartBlobStoreOutputStreamTest extends AnyFlatSpec with MockitoSugar with Matchers {

  private val MinFileSizeBytes = 10

  private val testBucketAndPath = BucketAndPath("mybucket", "mypath")

  /**
    * Create a byte array consisting of a given number of a repeating characters
    *
    * @param n    number of repetitions
    * @param char which character to repeat
    * @return new byte array
    */
  private def nBytes(n: Int, char: Char): Array[Byte] = {
    nString(n, char).getBytes
  }


  "write" should "setup the state on the construction" in new TestContext {
    verify(mockStorageInterface, times(1)).initUpload(testBucketAndPath)
  }

  "write" should "append directly to the buffer for bytes smaller than remaining buffer size" in new TestContext {
    val bytesToUpload: Array[Byte] = "Sausages".getBytes

    target.write(bytesToUpload, 0, bytesToUpload.length)

    verify(mockStorageInterface, never).uploadPart(
      any[MultiPartUploadState],
      any[Array[Byte]],
      ArgumentMatchers.eq(8)
    )
  }

  "write" should "start clear buffer and write part when nearly full" in new TestContext {

    target.write(nBytes(8, 'X'), 0, 8)
    target.write(nBytes(5, 'Y'), 0, 5)

    new String(target.getCurrentBufferContents).trim should be("YYY")

    verify(mockStorageInterface).uploadPart(
      any[MultiPartUploadState],
      any[Array[Byte]],
      ArgumentMatchers.eq(10L)
    )
  }

  "write" should "put multiple parts when exceeds buffer" in new TestContext {
    val payloadCaptor: ArgumentCaptor[Array[Byte]] = setUpUploadPartPayloadCaptor(mock[MultiPartUploadState])

    target.write(nBytes(8, 'X'), 0, 8)
    target.write(nBytes(12, 'Y') ++ nBytes(15, 'Z'), 0, 27)

    new String(target.getCurrentBufferContents).trim should be("ZZZZZ")

    val submittedPayloads: Seq[Array[Byte]] = payloadCaptor.getAllValues.asScala.toList
    submittedPayloads should have size 3
    new String(submittedPayloads(0)) should be("XXXXXXXXYY")
    new String(submittedPayloads(1)) should be("YYYYYYYYYY")
    new String(submittedPayloads(2)) should be("ZZZZZZZZZZ")
  }

  "write" should "put multiple parts when data exceeded buffer" in new TestContext {
    val payloadCaptor: ArgumentCaptor[Array[Byte]] = setUpUploadPartPayloadCaptor(mock[MultiPartUploadState])

    target.write(nBytes(8, 'X'), 0, 8)
    target.write(nBytes(12, 'Y'), 0, 12)

    new String(target.getCurrentBufferContents).trim should be("")

    val submittedPayloads: Seq[Array[Byte]] = payloadCaptor.getAllValues.asScala.toList
    submittedPayloads should have size 2
    new String(submittedPayloads(0)) should be("XXXXXXXXYY")
    new String(submittedPayloads(1)) should be("YYYYYYYYYY")
  }

  "complete" should "add a multipart file part if data remains in the buffer" in new TestContext {

    val returnPart: MultipartPart = mock[MultipartPart]
    val returnState: MultiPartUploadState = MultiPartUploadState(mock[MultipartUpload], Vector(returnPart))
    val payloadCaptor: ArgumentCaptor[Array[Byte]] = setUpUploadPartPayloadCaptor(returnState)

    target.write(nBytes(8, 'X'), 0, 8)
    target.complete()

    val submittedPayloads: Seq[Array[Byte]] = payloadCaptor.getAllValues.asScala.toList
    submittedPayloads should have size 1
    new String(submittedPayloads(0)).trim() should be("XXXXXXXX")
  }

  "complete" should "complete the upload if no data remains in the buffer" in new TestContext {

    val returnPart: MultipartPart = mock[MultipartPart]
    val returnState: MultiPartUploadState = MultiPartUploadState(mock[MultipartUpload], Vector(returnPart))
    val payloadCaptor: ArgumentCaptor[Array[Byte]] = setUpUploadPartPayloadCaptor(returnState)

    target.write(nBytes(10, 'X'), 0, 10)

    reset(mockStorageInterface)

    target.complete()

    verify(mockStorageInterface, never).uploadPart(any[MultiPartUploadState], payloadCaptor.capture(), ArgumentMatchers.eq(8))

  }

  /**
    * Create a string consisting of a given number of a repeating characters
    *
    * @param n    number of repetitions
    * @param char which character to repeat
    * @return new string
    */
  private def nString(n: Int, char: Char): String = {
    Array.fill(n)(char).mkString
  }

  class TestContext {

    implicit val mockStorageInterface: StorageInterface = mock[StorageInterface]
    val target = new MultipartBlobStoreOutputStream(testBucketAndPath, MinFileSizeBytes)
    private val multipartUpload: MultipartUpload = mock[MultipartUpload]

    private val initUploadState = MultiPartUploadState(multipartUpload, Vector())

    when(mockStorageInterface.initUpload(testBucketAndPath)).thenReturn(
      initUploadState
    )

    def setUpUploadPartPayloadCaptor(returnState: MultiPartUploadState): ArgumentCaptor[Array[Byte]] = {

      val payloadCaptor: ArgumentCaptor[Array[Byte]] = ArgumentCaptor.forClass(classOf[Array[Byte]])
      when(mockStorageInterface.uploadPart(
        any[MultiPartUploadState],
        payloadCaptor.capture(),
        anyLong()
      )).thenReturn(returnState)

      payloadCaptor
    }

  }


}
