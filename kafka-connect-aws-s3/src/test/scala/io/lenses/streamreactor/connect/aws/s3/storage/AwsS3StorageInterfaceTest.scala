/*
 * Copyright 2017-2026 Lenses.io Ltd
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

import cats.implicits.none
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.storage.FileMoveError
import org.mockito.ArgumentMatcher
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CopyObjectRequest
import software.amazon.awssdk.services.s3.model.CopyObjectResponse
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception

/**
 * The mvFile-based tests intentionally avoid `mockito-scala` (`MockitoSugar` /
 * `ArgumentMatchersSugar`). Those helpers trigger Scala runtime reflection on
 * the mocked type's method signatures; AWS SDK v2 response types declare a
 * recursively-bounded nested `Builder` interface
 * (`Builder extends CopyableBuilder<Builder, HeadObjectResponse>`), which the
 * Scala reflection machinery cannot fully resolve on every classloader layout
 * and surfaces as `CyclicReference: illegal cyclic reference involving class
 * HeadObjectResponse` in CI. Plain Java Mockito sidesteps the Scala symbol
 * table entirely and is stable across environments.
 */
class AwsS3StorageInterfaceTest extends AnyFlatSpecLike with Matchers with EitherValues {

  private def newMockS3Client(): S3Client        = Mockito.mock(classOf[S3Client])
  private def newMockTaskId():   ConnectorTaskId = Mockito.mock(classOf[ConnectorTaskId])

  private def anyHeadObjectRequest():   HeadObjectRequest   = ArgumentMatchers.any(classOf[HeadObjectRequest])
  private def anyCopyObjectRequest():   CopyObjectRequest   = ArgumentMatchers.any(classOf[CopyObjectRequest])
  private def anyDeleteObjectRequest(): DeleteObjectRequest = ArgumentMatchers.any(classOf[DeleteObjectRequest])

  private def headRequestKeyMatcher(key: String): HeadObjectRequest =
    ArgumentMatchers.argThat(new ArgumentMatcher[HeadObjectRequest] {
      override def matches(req: HeadObjectRequest): Boolean = req != null && req.key() == key
    })

  "mvFile" should "move a file from one bucket to another successfully" in {
    val s3Client         = newMockS3Client()
    val storageInterface = new AwsS3StorageInterface(newMockTaskId(), s3Client, batchDelete = false, None)

    Mockito.doReturn(HeadObjectResponse.builder().build()).when(s3Client).headObject(anyHeadObjectRequest())
    Mockito.doReturn(CopyObjectResponse.builder().build()).when(s3Client).copyObject(anyCopyObjectRequest())
    Mockito.doReturn(DeleteObjectResponse.builder().build()).when(s3Client).deleteObject(anyDeleteObjectRequest())

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result shouldBe Right(())
    Mockito.verify(s3Client).copyObject(anyCopyObjectRequest())
    Mockito.verify(s3Client).deleteObject(anyDeleteObjectRequest())
  }

  it should "return a FileMoveError if copyObject fails" in {
    val s3Client         = newMockS3Client()
    val storageInterface = new AwsS3StorageInterface(newMockTaskId(), s3Client, batchDelete = false, None)

    Mockito.doReturn(HeadObjectResponse.builder().build()).when(s3Client).headObject(anyHeadObjectRequest())
    Mockito.doThrow(new RuntimeException("Copy failed")).when(s3Client).copyObject(anyCopyObjectRequest())

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result.isLeft shouldBe true
    result.left.value shouldBe a[FileMoveError]
    Mockito.verify(s3Client).copyObject(anyCopyObjectRequest())
    Mockito.verify(s3Client, Mockito.never()).deleteObject(anyDeleteObjectRequest())
  }

  it should "return a FileMoveError if deleteObject fails" in {
    val s3Client         = newMockS3Client()
    val storageInterface = new AwsS3StorageInterface(newMockTaskId(), s3Client, batchDelete = false, None)

    Mockito.doReturn(HeadObjectResponse.builder().build()).when(s3Client).headObject(anyHeadObjectRequest())
    Mockito.doReturn(CopyObjectResponse.builder().build()).when(s3Client).copyObject(anyCopyObjectRequest())
    Mockito.doThrow(new RuntimeException("Delete failed")).when(s3Client).deleteObject(anyDeleteObjectRequest())

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result.isLeft shouldBe true
    result.left.value shouldBe a[FileMoveError]
    Mockito.verify(s3Client).copyObject(anyCopyObjectRequest())
    Mockito.verify(s3Client).deleteObject(anyDeleteObjectRequest())
  }

  it should "return Right when source is missing but destination already exists (idempotent replay)" in {
    // S1: a previous mvFile completed (source deleted, destination written). On
    // pending-state replay we may be asked to do the move again; the contract is
    // idempotent success, NOT silent skip-with-no-write.
    val s3Client         = newMockS3Client()
    val storageInterface = new AwsS3StorageInterface(newMockTaskId(), s3Client, batchDelete = false, None)

    Mockito.doThrow(NoSuchKeyException.builder().build())
      .when(s3Client).headObject(headRequestKeyMatcher("oldPath"))
    Mockito.doReturn(HeadObjectResponse.builder().build())
      .when(s3Client).headObject(headRequestKeyMatcher("newPath"))

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result shouldBe Right(())
    Mockito.verify(s3Client, Mockito.never()).copyObject(anyCopyObjectRequest())
    Mockito.verify(s3Client, Mockito.never()).deleteObject(anyDeleteObjectRequest())
  }

  it should "return FileMoveError when both source and destination are missing" in {
    // S1: nothing was ever written. The original implementation returned Right(())
    // here, which silently advanced the pending pipeline without writing the final
    // object -- silent data loss. We must return Left so the caller fails loudly.
    val s3Client         = newMockS3Client()
    val storageInterface = new AwsS3StorageInterface(newMockTaskId(), s3Client, batchDelete = false, None)

    Mockito.doThrow(NoSuchKeyException.builder().build()).when(s3Client).headObject(anyHeadObjectRequest())

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result.isLeft shouldBe true
    result.left.value shouldBe a[FileMoveError]
    Mockito.verify(s3Client, Mockito.never()).copyObject(anyCopyObjectRequest())
    Mockito.verify(s3Client, Mockito.never()).deleteObject(anyDeleteObjectRequest())
  }

  it should "return Right when source raises S3Exception(404) but destination exists (MinIO/LocalStack replay)" in {
    // S3-compatible backends (MinIO, LocalStack, Ceph) often surface a missing object
    // as a generic S3Exception with HTTP 404 rather than the typed NoSuchKeyException.
    // The idempotent-replay contract must still hold on those backends.
    val s3Client         = newMockS3Client()
    val storageInterface = new AwsS3StorageInterface(newMockTaskId(), s3Client, batchDelete = false, None)

    Mockito.doThrow(S3Exception.builder().statusCode(404).message("Not Found").build().asInstanceOf[Throwable])
      .when(s3Client).headObject(headRequestKeyMatcher("oldPath"))
    Mockito.doReturn(HeadObjectResponse.builder().build())
      .when(s3Client).headObject(headRequestKeyMatcher("newPath"))

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result shouldBe Right(())
    Mockito.verify(s3Client, Mockito.never()).copyObject(anyCopyObjectRequest())
    Mockito.verify(s3Client, Mockito.never()).deleteObject(anyDeleteObjectRequest())
  }

  it should "return FileMoveError when both source and destination raise S3Exception(404)" in {
    // Same both-missing contract as the NoSuchKeyException case, but via the generic
    // S3Exception path produced by MinIO/LocalStack-style backends.
    val s3Client         = newMockS3Client()
    val storageInterface = new AwsS3StorageInterface(newMockTaskId(), s3Client, batchDelete = false, None)

    Mockito.doThrow(S3Exception.builder().statusCode(404).message("Not Found").build().asInstanceOf[Throwable])
      .when(s3Client).headObject(anyHeadObjectRequest())

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result.isLeft shouldBe true
    result.left.value shouldBe a[FileMoveError]
    result.left.value.exception shouldBe a[IllegalStateException]
    Mockito.verify(s3Client, Mockito.never()).copyObject(anyCopyObjectRequest())
    Mockito.verify(s3Client, Mockito.never()).deleteObject(anyDeleteObjectRequest())
  }
}
