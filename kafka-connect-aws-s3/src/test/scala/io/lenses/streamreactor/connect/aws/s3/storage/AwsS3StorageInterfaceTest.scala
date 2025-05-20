/*
 * Copyright 2017-2025 Lenses.io Ltd
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

class AwsS3StorageInterfaceTest
    extends AnyFlatSpecLike
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with EitherValues {

  "mvFile" should "move a file from one bucket to another successfully" in {
    val s3Client         = mock[S3Client]
    val storageInterface = new AwsS3StorageInterface(mock[ConnectorTaskId], s3Client, batchDelete = false, None)

    when(s3Client.copyObject(any[CopyObjectRequest])).thenAnswer(CopyObjectResponse.builder().build())
    when(s3Client.deleteObject(any[DeleteObjectRequest])).thenAnswer(DeleteObjectResponse.builder().build())

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result shouldBe Right(())
    verify(s3Client).copyObject(any[CopyObjectRequest])
    verify(s3Client).deleteObject(any[DeleteObjectRequest])
  }

  it should "return a FileMoveError if copyObject fails" in {
    val s3Client         = mock[S3Client]
    val storageInterface = new AwsS3StorageInterface(mock[ConnectorTaskId], s3Client, batchDelete = false, None)

    when(s3Client.copyObject(any[CopyObjectRequest])).thenThrow(new RuntimeException("Copy failed"))

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result.isLeft shouldBe true
    result.left.value shouldBe a[FileMoveError]
    verify(s3Client).copyObject(any[CopyObjectRequest])
    verify(s3Client, never).deleteObject(any[DeleteObjectRequest])
  }

  it should "return a FileMoveError if deleteObject fails" in {
    val s3Client         = mock[S3Client]
    val storageInterface = new AwsS3StorageInterface(mock[ConnectorTaskId], s3Client, batchDelete = false, None)

    when(s3Client.headObject(any[HeadObjectRequest])).thenAnswer(HeadObjectResponse.builder().build())
    when(s3Client.copyObject(any[CopyObjectRequest])).thenAnswer(CopyObjectResponse.builder().build())
    when(s3Client.deleteObject(any[DeleteObjectRequest])).thenThrow(new RuntimeException("Delete failed"))

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result.isLeft shouldBe true
    result.left.value shouldBe a[FileMoveError]
    verify(s3Client).copyObject(any[CopyObjectRequest])
    verify(s3Client).deleteObject(any[DeleteObjectRequest])
  }

  it should "pass if no source object exists" in {
    val s3Client         = mock[S3Client]
    val storageInterface = new AwsS3StorageInterface(mock[ConnectorTaskId], s3Client, batchDelete = false, None)

    when(s3Client.headObject(any[HeadObjectRequest])).thenThrow(NoSuchKeyException.builder().build())
    when(s3Client.copyObject(any[CopyObjectRequest])).thenAnswer(CopyObjectResponse.builder().build())
    when(s3Client.deleteObject(any[DeleteObjectRequest])).thenThrow(new RuntimeException("Delete failed"))

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    result.isRight shouldBe true
    verify(s3Client).headObject(any[HeadObjectRequest])
    verify(s3Client, never).copyObject(any[CopyObjectRequest])
    verify(s3Client, never).deleteObject(any[DeleteObjectRequest])
  }
}
