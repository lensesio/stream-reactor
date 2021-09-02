
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

import io.lenses.streamreactor.connect.aws.s3.model.location.{RemoteS3PathLocation, RemoteS3RootLocation}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.RemoteFileTestHelper
import org.jclouds.blobstore.domain._
import org.jclouds.blobstore.domain.internal.{PageSetImpl, StorageMetadataImpl}
import org.jclouds.blobstore.options.{ListContainerOptions, PutOptions}
import org.jclouds.blobstore.{BlobStore, BlobStoreContext}
import org.jclouds.io.Payload
import org.jclouds.io.payloads.{ByteSourcePayload, InputStreamPayload}
import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, ArgumentMatchers, MockitoSugar}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class JCloudsStorageInterfaceTest extends AnyFlatSpec with MockitoSugar with Matchers with BeforeAndAfter {

  private val blobStoreContext: BlobStoreContext = mock[BlobStoreContext]
  private val blobStore: BlobStore = mock[BlobStore]
  private val testBucketAndPath = RemoteS3PathLocation("my-bucket", "myPath")

  when(blobStoreContext.getBlobStore).thenReturn(blobStore)


  private implicit val jCloudsStorageInterface: JCloudsStorageInterface = new JCloudsStorageInterface("test", blobStoreContext) {
    override def pathExists(bucketAndPrefix: RemoteS3RootLocation): Either[Throwable, Boolean] = ???

    override def list(bucketAndPrefix: RemoteS3RootLocation, lastFile: Option[RemoteS3PathLocation], numResults: Int): Either[Throwable, List[String]] = ???
  }

  private val payloadReader = new RemoteFileTestHelper
  import payloadReader._

  before {
    reset(blobStore)
  }

  "initUpload" should "initialise upload and create state" in {

    val multipartUpload = mock[MultipartUpload]

    when(blobStore.initiateMultipartUpload(anyString(), any(classOf[BlobMetadata]), any(classOf[PutOptions])))
      .thenReturn(multipartUpload)

    val newState = jCloudsStorageInterface.initUpload(testBucketAndPath)

    newState.parts should be(List())
  }

  "uploadPart" should "call blob store and add part to the state" in {

    val multipartPart = mock[MultipartPart]
    val payloadCaptor: ArgumentCaptor[Payload] = ArgumentCaptor.forClass(classOf[ByteSourcePayload])
    when(blobStore.uploadMultipartPart(any(classOf[MultipartUpload]), anyInt(), payloadCaptor.capture())).thenReturn(multipartPart)

    val uploadState = createUploadState(existingBufferBytes = nBytes(8, 'X'))

    val bytesToUpload = "Sausages".getBytes()

    val updatedState = jCloudsStorageInterface.uploadPart(uploadState, bytesToUpload)
    updatedState.parts should contain(multipartPart)

    val submittedPayloads: List[Payload] = payloadCaptor.getAllValues.asScala.toList
    submittedPayloads should have size 1
    streamToString(submittedPayloads.head.openStream()) should be("Sausages")
  }

  "completeUpload" should "complete the upload" in {

    val uploadState: MultiPartUploadState = createUploadState()

    jCloudsStorageInterface.completeUpload(uploadState)

    verify(blobStore).completeMultipartUpload(
      any(classOf[MultipartUpload]),
      any(classOf[java.util.List[MultipartPart]])
    )

  }

  def mockStorageMetadata(first: String, nextMarker: String): PageSetImpl[StorageMetadata] = {
    val storageMetadata1: StorageMetadata = mock[StorageMetadataImpl]
    when(storageMetadata1.getName).thenReturn(first)
    when(storageMetadata1.getType).thenReturn(StorageType.BLOB)
    new PageSetImpl(List(storageMetadata1).asJava, nextMarker)
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
