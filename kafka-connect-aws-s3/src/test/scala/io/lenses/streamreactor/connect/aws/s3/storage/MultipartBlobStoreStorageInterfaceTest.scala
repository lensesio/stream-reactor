
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

import io.lenses.streamreactor.connect.aws.s3.BucketAndPath
import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3TestPayloadReader
import org.jclouds.blobstore.domain.{BlobMetadata, MultipartPart, MultipartUpload}
import org.jclouds.blobstore.options.PutOptions
import org.jclouds.blobstore.{BlobStore, BlobStoreContext}
import org.jclouds.io.payloads.ByteSourcePayload
import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class MultipartBlobStoreStorageInterfaceTest extends AnyFlatSpec with MockitoSugar with Matchers with BeforeAndAfter {

  private val blobStoreContext: BlobStoreContext = mock[BlobStoreContext]
  private val blobStore: BlobStore = mock[BlobStore]
  private val testBucketAndPath = BucketAndPath("mybucket", "mypath")

  when(blobStoreContext.getBlobStore).thenReturn(blobStore)

  private val multipartBlobStoreStorageInterface = new MultipartBlobStoreStorageInterface(blobStoreContext)

  before {
    reset(blobStore)
  }

  "initUpload" should "initialise upload and create state" in {

    val multipartUpload = mock[MultipartUpload]

    when(blobStore.initiateMultipartUpload(anyString(), any(classOf[BlobMetadata]), any(classOf[PutOptions])))
      .thenReturn(multipartUpload)

    val newState = multipartBlobStoreStorageInterface.initUpload(testBucketAndPath)

    newState.parts should be(List())
  }

  "uploadPart" should "call blob store and add part to the state" in {

    val multipartPart = mock[MultipartPart]
    val payloadCaptor: ArgumentCaptor[ByteSourcePayload] = ArgumentCaptor.forClass(classOf[ByteSourcePayload])
    when(blobStore.uploadMultipartPart(any(classOf[MultipartUpload]), anyInt(), payloadCaptor.capture())).thenReturn(multipartPart)

    val uploadState = createUploadState(existingBufferBytes = nBytes(8, 'X'))

    val bytesToUpload = "Sausages".getBytes()

    val updatedState = multipartBlobStoreStorageInterface.uploadPart(uploadState, bytesToUpload, bytesToUpload.length)
    updatedState.parts should contain(multipartPart)

    val submittedPayloads: Seq[ByteSourcePayload] = payloadCaptor.getAllValues.asScala.toList
    submittedPayloads should have size 1
    S3TestPayloadReader.extractPayload(submittedPayloads(0)) should be("Sausages")
  }

  "completeUpload" should "complete the upload" in {

    val uploadState: MultiPartUploadState = createUploadState()

    multipartBlobStoreStorageInterface.completeUpload(uploadState)

    verify(blobStore).completeMultipartUpload(
      any(classOf[MultipartUpload]),
      any(classOf[java.util.List[MultipartPart]])
    )

  }

  private def createUploadState(
                                 existingParts: Vector[MultipartPart] = Vector(),
                                 existingBufferBytes: Array[Byte] = Array()
                               ) = {
    val upload = mock[MultipartUpload]
    val parts = existingParts

    MultiPartUploadState(upload, parts)
  }

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


}
