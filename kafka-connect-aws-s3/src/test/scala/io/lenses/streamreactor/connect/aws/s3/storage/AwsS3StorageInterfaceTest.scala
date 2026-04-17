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
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
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

class AwsS3StorageInterfaceTest
    extends AnyFlatSpecLike
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with EitherValues {

  "mvFile" should "move a file from one bucket to another successfully" in {
    val s3Client         = mock[S3Client]
    val storageInterface = new AwsS3StorageInterface(mock[ConnectorTaskId], s3Client, batchDelete = false, None)

    org.mockito.Mockito.doReturn(HeadObjectResponse.builder().build()).when(s3Client).headObject(any[HeadObjectRequest])
    org.mockito.Mockito.doReturn(CopyObjectResponse.builder().build()).when(s3Client).copyObject(any[CopyObjectRequest])
    org.mockito.Mockito.doReturn(DeleteObjectResponse.builder().build()).when(s3Client).deleteObject(
      any[DeleteObjectRequest],
    )

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result shouldBe Right(())
    verify(s3Client).copyObject(any[CopyObjectRequest])
    verify(s3Client).deleteObject(any[DeleteObjectRequest])
  }

  it should "return a FileMoveError if copyObject fails" in {
    val s3Client         = mock[S3Client]
    val storageInterface = new AwsS3StorageInterface(mock[ConnectorTaskId], s3Client, batchDelete = false, None)

    org.mockito.Mockito.doThrow(new RuntimeException("Copy failed")).when(s3Client).copyObject(any[CopyObjectRequest])

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result.isLeft shouldBe true
    result.left.value shouldBe a[FileMoveError]
    verify(s3Client).copyObject(any[CopyObjectRequest])
    verify(s3Client, never).deleteObject(any[DeleteObjectRequest])
  }

  it should "return a FileMoveError if deleteObject fails" in {
    val s3Client         = mock[S3Client]
    val storageInterface = new AwsS3StorageInterface(mock[ConnectorTaskId], s3Client, batchDelete = false, None)

    org.mockito.Mockito.doReturn(HeadObjectResponse.builder().build()).when(s3Client).headObject(any[HeadObjectRequest])
    org.mockito.Mockito.doReturn(CopyObjectResponse.builder().build()).when(s3Client).copyObject(any[CopyObjectRequest])
    org.mockito.Mockito.doThrow(new RuntimeException("Delete failed")).when(s3Client).deleteObject(
      any[DeleteObjectRequest],
    )

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result.isLeft shouldBe true
    result.left.value shouldBe a[FileMoveError]
    verify(s3Client).copyObject(any[CopyObjectRequest])
    verify(s3Client).deleteObject(any[DeleteObjectRequest])
  }

  it should "return Right when source is missing but destination already exists (idempotent replay)" in {
    // S1: a previous mvFile completed (source deleted, destination written). On
    // pending-state replay we may be asked to do the move again; the contract is
    // idempotent success, NOT silent skip-with-no-write.
    val s3Client         = mock[S3Client]
    val storageInterface = new AwsS3StorageInterface(mock[ConnectorTaskId], s3Client, batchDelete = false, None)

    val sourceMatcher =
      new org.mockito.ArgumentMatcher[HeadObjectRequest] {
        override def matches(req: HeadObjectRequest): Boolean = req != null && req.key() == "oldPath"
      }
    val destMatcher =
      new org.mockito.ArgumentMatcher[HeadObjectRequest] {
        override def matches(req: HeadObjectRequest): Boolean = req != null && req.key() == "newPath"
      }

    org.mockito.Mockito.doThrow(NoSuchKeyException.builder().build())
      .when(s3Client).headObject(org.mockito.ArgumentMatchers.argThat(sourceMatcher))
    org.mockito.Mockito.doReturn(HeadObjectResponse.builder().build())
      .when(s3Client).headObject(org.mockito.ArgumentMatchers.argThat(destMatcher))

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result shouldBe Right(())
    verify(s3Client, never).copyObject(any[CopyObjectRequest])
    verify(s3Client, never).deleteObject(any[DeleteObjectRequest])
  }

  it should "return FileMoveError when both source and destination are missing" in {
    // S1: nothing was ever written. The original implementation returned Right(())
    // here, which silently advanced the pending pipeline without writing the final
    // object -- silent data loss. We must return Left so the caller fails loudly.
    val s3Client         = mock[S3Client]
    val storageInterface = new AwsS3StorageInterface(mock[ConnectorTaskId], s3Client, batchDelete = false, None)

    org.mockito.Mockito.doThrow(NoSuchKeyException.builder().build()).when(s3Client).headObject(any[HeadObjectRequest])

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result.isLeft shouldBe true
    result.left.value shouldBe a[FileMoveError]
    verify(s3Client, never).copyObject(any[CopyObjectRequest])
    verify(s3Client, never).deleteObject(any[DeleteObjectRequest])
  }

  it should "return Right when source raises S3Exception(404) but destination exists (MinIO/LocalStack replay)" in {
    // S3-compatible backends (MinIO, LocalStack, Ceph) often surface a missing object
    // as a generic S3Exception with HTTP 404 rather than the typed NoSuchKeyException.
    // The idempotent-replay contract must still hold on those backends.
    val s3Client         = mock[S3Client]
    val storageInterface = new AwsS3StorageInterface(mock[ConnectorTaskId], s3Client, batchDelete = false, None)

    val sourceMatcher =
      new org.mockito.ArgumentMatcher[HeadObjectRequest] {
        override def matches(req: HeadObjectRequest): Boolean = req != null && req.key() == "oldPath"
      }
    val destMatcher =
      new org.mockito.ArgumentMatcher[HeadObjectRequest] {
        override def matches(req: HeadObjectRequest): Boolean = req != null && req.key() == "newPath"
      }

    org.mockito.Mockito.doThrow(
      S3Exception.builder().statusCode(404).message("Not Found").build().asInstanceOf[Throwable],
    )
      .when(s3Client).headObject(org.mockito.ArgumentMatchers.argThat(sourceMatcher))
    org.mockito.Mockito.doReturn(HeadObjectResponse.builder().build())
      .when(s3Client).headObject(org.mockito.ArgumentMatchers.argThat(destMatcher))

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result shouldBe Right(())
    verify(s3Client, never).copyObject(any[CopyObjectRequest])
    verify(s3Client, never).deleteObject(any[DeleteObjectRequest])
  }

  it should "return FileMoveError when both source and destination raise S3Exception(404)" in {
    // Same both-missing contract as the NoSuchKeyException case, but via the generic
    // S3Exception path produced by MinIO/LocalStack-style backends.
    val s3Client         = mock[S3Client]
    val storageInterface = new AwsS3StorageInterface(mock[ConnectorTaskId], s3Client, batchDelete = false, None)

    org.mockito.Mockito.doThrow(
      S3Exception.builder().statusCode(404).message("Not Found").build().asInstanceOf[Throwable],
    )
      .when(s3Client).headObject(any[HeadObjectRequest])

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result.isLeft shouldBe true
    result.left.value shouldBe a[FileMoveError]
    result.left.value.exception shouldBe a[IllegalStateException]
    verify(s3Client, never).copyObject(any[CopyObjectRequest])
    verify(s3Client, never).deleteObject(any[DeleteObjectRequest])
  }
}
