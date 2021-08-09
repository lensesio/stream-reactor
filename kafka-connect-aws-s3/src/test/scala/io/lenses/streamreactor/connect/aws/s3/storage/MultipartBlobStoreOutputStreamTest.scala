
/*
 * Copyright 2020 Lenses.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.lenses.streamreactor.connect.aws.s3.storage

import io.lenses.streamreactor.connect.aws.s3.model.{Offset, RemotePathLocation}
import io.lenses.streamreactor.connect.aws.s3.processing._
import org.jclouds.blobstore.domain.MultipartUpload
import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class MultipartBlobStoreOutputStreamTest extends AnyFlatSpec with MockitoSugar with Matchers {

  private val MinFileSizeBytes = 10

  private val testBucketAndPath = RemotePathLocation("my-bucket", "my-path")

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


  "write" should "queue up creation of the state on the construction" in new TestContext {
    verify(queueProcessor, times(1)).enqueue(any[InitUploadProcessorOperation])
    verifyNoMoreInteractions()
  }

  "write" should "append directly to the buffer for bytes smaller than remaining buffer size" in new TestContext {
    val bytesToUpload: Array[Byte] = "Sausages".getBytes

    target.write(bytesToUpload, 0, bytesToUpload.length)

    val order = inOrder(queueProcessor)
    order.verify(queueProcessor).enqueue(any[InitUploadProcessorOperation])
    order.verify(queueProcessor, never).enqueue(any[UploadPartProcessorOperation])
    order.verify(queueProcessor, never).enqueue(any[CompleteUploadProcessorOperation])
    order.verifyNoMoreInteractions()
  }

  "write" should "start clear buffer and write part when nearly full" in new TestContext {

    target.write(nBytes(8, 'X'), 0, 8)
    target.write(nBytes(5, 'Y'), 0, 5)

    new String(target.getCurrentBufferContents).trim should be("YYY")

    val order = inOrder(queueProcessor)
    order.verify(queueProcessor).enqueue(any(classOf[InitUploadProcessorOperation]))
    order.verify(queueProcessor).enqueue(any(classOf[UploadPartProcessorOperation]))
    order.verifyNoMoreInteractions()

    matchOperationData(0, "XXXXXXXXYY")

  }

  "write" should "put multiple parts when exceeds buffer" in new TestContext {
    val upload = mock[MultipartUpload]
    val updatedState = mock[MultiPartUploadState]
    when(updatedState.upload).thenReturn(upload)

    target.write(nBytes(8, 'X'), 0, 8)
    target.write(nBytes(12, 'Y') ++ nBytes(15, 'Z'), 0, 27)

    new String(target.getCurrentBufferContents).trim should be("ZZZZZ")

    val order = inOrder(queueProcessor)
    order.verify(queueProcessor).enqueue(any(classOf[InitUploadProcessorOperation]))
    order.verify(queueProcessor, times(3)).enqueue(any(classOf[UploadPartProcessorOperation]))
    order.verifyNoMoreInteractions()

    matchOperationData(0, "XXXXXXXXYY")
    matchOperationData(1, "YYYYYYYYYY")
    matchOperationData(2, "ZZZZZZZZZZ")

  }

  "write" should "put multiple parts when data exceeded buffer" in new TestContext {

    target.write(nBytes(8, 'X'), 0, 8)
    target.write(nBytes(12, 'Y'), 0, 12)

    new String(target.getCurrentBufferContents).trim should be("")

    matchOperationData(0, "XXXXXXXXYY")
    matchOperationData(1, "YYYYYYYYYY")

  }

  "complete" should "add a multipart file part if data remains in the buffer" in new TestContext {

    target.write(nBytes(8, 'X'), 0, 8)
    target.complete(testBucketAndPath, Offset(0))

    matchOperationData(0, "XXXXXXXX")

  }

  "complete" should "complete the upload if no data remains in the buffer" in new TestContext {

    target.write(nBytes(10, 'X'), 0, 10)
    target.complete(testBucketAndPath, Offset(0))

    target.getCurrentBufferContents should be (empty)
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

    implicit val queueProcessor = mock[BlockingQueueProcessor]
    when(queueProcessor.hasOperations).thenReturn(false)

    val target = new MultipartBlobStoreOutputStream(testBucketAndPath, Offset(0), minAllowedMultipartSize = MinFileSizeBytes, updateOffsetFn = (_) => () => ())

    private val multipartUpload: MultipartUpload = mock[MultipartUpload]
    when(multipartUpload.containerName()).thenReturn("myContainerName")
    when(multipartUpload.blobName()).thenReturn("myBlobName")

    val opDataCaptor = opDataPayloadCaptor()
    def opDataPayloadCaptor(): ArgumentCaptor[ProcessorOperation] = {

      val payloadCaptor: ArgumentCaptor[ProcessorOperation] = ArgumentCaptor.forClass(classOf[ProcessorOperation])
      doNothing.when(queueProcessor).enqueue(payloadCaptor.capture())

      payloadCaptor
    }

    def matchOperationData(index: Int, expectedString : String) = {
      val operationData = opDataCaptor.getAllValues.asScala.toList
      operationData(index) match {
        case UploadPartProcessorOperation(_, _, bytes, _) => bytes should be(expectedString.getBytes)
        case _ => fail("wrong match")
      }
    }
  }
}
