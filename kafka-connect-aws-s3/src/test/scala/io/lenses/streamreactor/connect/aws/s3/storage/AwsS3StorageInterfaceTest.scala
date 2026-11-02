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
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.sink.seek.NoOverwriteExistingObject
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectWithETag
import io.lenses.streamreactor.connect.cloud.common.storage.FileCreateError
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.FileMoveError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfKeysResponse
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfMetadataResponse
import io.lenses.streamreactor.connect.cloud.common.storage.NonOverwriteFileExistsError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterfaceListFilteringBehaviour
import org.mockito.ArgumentMatcher
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CopyObjectRequest
import software.amazon.awssdk.services.s3.model.CopyObjectResponse
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable

import java.time.Instant
import scala.jdk.CollectionConverters.SeqHasAsJava

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
class AwsS3StorageInterfaceTest
    extends AnyFlatSpecLike
    with Matchers
    with EitherValues
    with StorageInterfaceListFilteringBehaviour {

  private def newMockS3Client(): S3Client        = Mockito.mock(classOf[S3Client])
  private def newMockTaskId():   ConnectorTaskId = Mockito.mock(classOf[ConnectorTaskId])

  private def anyHeadObjectRequest():   HeadObjectRequest   = ArgumentMatchers.any(classOf[HeadObjectRequest])
  private def anyCopyObjectRequest():   CopyObjectRequest   = ArgumentMatchers.any(classOf[CopyObjectRequest])
  private def anyDeleteObjectRequest(): DeleteObjectRequest = ArgumentMatchers.any(classOf[DeleteObjectRequest])
  private def anyListObjectsV2Request(): ListObjectsV2Request =
    ArgumentMatchers.any(classOf[ListObjectsV2Request])

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

  // ── Permanent copyObject error (KMS-style) ────────────────────────────────────────────────

  it should "return FileMoveError when copyObject throws a permanent S3Exception (KMS-style 400) and NOT treat it as idempotent success" in {
    // Motivation (from docs/datalake-exactly-once-partitionby.md): a KMS key being disabled
    // produces a permanent error from S3 during the Copy phase (HTTP 400 / KMSDisabledException).
    // Unlike the idempotent-replay 404 path, this error MUST propagate as Left(FileMoveError)
    // so the sink escalates to FatalCloudSinkError and the LC-2451 in-place rollback path runs.
    //
    // This test complements the existing 404 idempotence test by covering the symmetric
    // permanent-error case. The key distinction: the destination is NOT checked when copyObject
    // itself fails (the source headObject succeeded, so the 404 idempotence branch is skipped).
    val s3Client         = newMockS3Client()
    val storageInterface = new AwsS3StorageInterface(newMockTaskId(), s3Client, batchDelete = false, None)

    // Source exists: headObject succeeds.
    Mockito.doReturn(HeadObjectResponse.builder().build())
      .when(s3Client).headObject(anyHeadObjectRequest())

    // copyObject fails with a permanent 400 (KMS key disabled simulation).
    val kmsError = S3Exception.builder()
      .statusCode(400)
      .message("KMS.KMSDisabledException: The specified KMS key has been disabled")
      .build()
      .asInstanceOf[Throwable]
    Mockito.doThrow(kmsError).when(s3Client).copyObject(anyCopyObjectRequest())

    val result = storageInterface.mvFile("oldBucket", "oldPath", "newBucket", "newPath", none)

    // Permanent error: Left(FileMoveError) — NOT silently treated as idempotent success.
    result.isLeft shouldBe true
    result.left.value shouldBe a[FileMoveError]

    // copyObject was attempted (source existed), but delete was NOT called (copy failed first).
    Mockito.verify(s3Client).copyObject(anyCopyObjectRequest())
    Mockito.verify(s3Client, Mockito.never()).deleteObject(anyDeleteObjectRequest())

    // Crucially: the destination head was NOT inspected (no idempotence bypass for 400 errors).
    Mockito.verify(s3Client, Mockito.times(1)).headObject(anyHeadObjectRequest())
  }

  // ── writeBlobToFile — lost no-overwrite create race ──────────────────────

  private case class TestIndexFile(owner: String, offset: Option[Long])
  private implicit val testIndexFileEncoder: Encoder[TestIndexFile] = deriveEncoder[TestIndexFile]

  private def anyPutObjectRequest(): PutObjectRequest = ArgumentMatchers.any(classOf[PutObjectRequest])
  private def anyRequestBody():      RequestBody      = ArgumentMatchers.any(classOf[RequestBody])

  private def s3ExceptionWith(errorCode: String, status: Int): S3Exception =
    S3Exception.builder()
      .statusCode(status)
      .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
      .build().asInstanceOf[S3Exception]

  "writeBlobToFile" should "return NonOverwriteFileExistsError when NoOverwriteExistingObject loses the race (PreconditionFailed)" in {
    val s3Client         = newMockS3Client()
    val storageInterface = new AwsS3StorageInterface(newMockTaskId(), s3Client, batchDelete = false, None)

    Mockito.doThrow(s3ExceptionWith("PreconditionFailed", 412))
      .when(s3Client).putObject(anyPutObjectRequest(), anyRequestBody())

    val result =
      storageInterface.writeBlobToFile("bucket", "path", NoOverwriteExistingObject(TestIndexFile("o", Some(1L))))

    result.isLeft shouldBe true
    result.left.value shouldBe a[NonOverwriteFileExistsError]
  }

  it should "return FileCreateError (not NonOverwriteFileExistsError) when ObjectWithETag and PreconditionFailed — fencing must stay fatal" in {
    val s3Client         = newMockS3Client()
    val storageInterface = new AwsS3StorageInterface(newMockTaskId(), s3Client, batchDelete = false, None)

    Mockito.doThrow(s3ExceptionWith("PreconditionFailed", 412))
      .when(s3Client).putObject(anyPutObjectRequest(), anyRequestBody())

    val result =
      storageInterface.writeBlobToFile("bucket", "path", ObjectWithETag(TestIndexFile("o", Some(1L)), "etag"))

    result.isLeft shouldBe true
    result.left.value shouldBe a[FileCreateError]
  }

  it should "return FileCreateError when NoOverwriteExistingObject fails with a non-precondition error" in {
    val s3Client         = newMockS3Client()
    val storageInterface = new AwsS3StorageInterface(newMockTaskId(), s3Client, batchDelete = false, None)

    Mockito.doThrow(s3ExceptionWith("InternalError", 500))
      .when(s3Client).putObject(anyPutObjectRequest(), anyRequestBody())

    val result =
      storageInterface.writeBlobToFile("bucket", "path", NoOverwriteExistingObject(TestIndexFile("o", Some(1L))))

    result.isLeft shouldBe true
    result.left.value shouldBe a[FileCreateError]
  }

  // ── LC-318: shared list-filtering contract (StorageInterfaceListFilteringBehaviour) ──────

  private val filterTestBucket = "filter-test-bucket"

  private def s3ObjectFor(obj: FilterFixtureObject): S3Object =
    S3Object.builder().key(obj.key).size(obj.sizeBytes).lastModified(Instant.EPOCH).build()

  /**
   * Wires a fresh mocked S3Client + AwsS3StorageInterface returning exactly `objects` for both
   * a single listObjectsV2 call and its paginator (used by the recursive listing methods).
   */
  private def newStorageInterfaceReturning(objects: Seq[FilterFixtureObject]): AwsS3StorageInterface = {
    val s3Client         = newMockS3Client()
    val storageInterface = new AwsS3StorageInterface(newMockTaskId(), s3Client, batchDelete = false, None)

    val response = ListObjectsV2Response.builder().contents(objects.map(s3ObjectFor).asJava).build()
    Mockito.doReturn(response).when(s3Client).listObjectsV2(anyListObjectsV2Request())

    // The paginator is a real ListObjectsV2Iterable backed by the same mocked client, so it
    // drives its pagination through the listObjectsV2 stub above (a single page, since the
    // response above has no nextContinuationToken).
    val paginator = new ListObjectsV2Iterable(s3Client, ListObjectsV2Request.builder().bucket(filterTestBucket).build())
    Mockito.doReturn(paginator).when(s3Client).listObjectsV2Paginator(anyListObjectsV2Request())

    storageInterface
  }

  override def listWithFixture(
    objects: Seq[FilterFixtureObject],
  ): Either[FileListError, Option[ListOfKeysResponse[_]]] =
    newStorageInterfaceReturning(objects).list(filterTestBucket, none, none, objects.size)

  override def listKeysRecursiveWithFixture(
    objects: Seq[FilterFixtureObject],
  ): Either[FileListError, Option[ListOfKeysResponse[_]]] =
    newStorageInterfaceReturning(objects).listKeysRecursive(filterTestBucket, none)

  override def listFileMetaRecursiveWithFixture(
    objects: Seq[FilterFixtureObject],
  ): Either[FileListError, Option[ListOfMetadataResponse[_ <: FileMetadata]]] =
    newStorageInterfaceReturning(objects).listFileMetaRecursive(filterTestBucket, none)

  // ── LC-344: an all-filtered page must still advance the resume marker ─────────────────

  "list" should "advance the resume marker past a page consisting entirely of markers/zero-byte objects" in {
    // The marker comes from the raw last object of the page, as on GCS, so a page with nothing
    // readable on it does not make every later listing return that same page forever.
    val allFiltered = Seq(FilterFixtureObject(zeroByteKey, 0L), FilterFixtureObject(markerKey, 42L))
    val response = newStorageInterfaceReturning(allFiltered)
      .list(filterTestBucket, none, none, allFiltered.size)
      .value
      .value
    response.files shouldBe empty
    response.latestFileMetadata.file shouldBe markerKey
  }

  it should "keep the last readable key as the resume marker when the page ends with one" in {
    val page = Seq(FilterFixtureObject(zeroByteKey, 0L), FilterFixtureObject(validKey, 42L))
    val response = newStorageInterfaceReturning(page)
      .list(filterTestBucket, none, none, page.size)
      .value
      .value
    response.files should contain only validKey
    response.latestFileMetadata.file shouldBe validKey
  }

  it should "return None when the page is empty" in {
    newStorageInterfaceReturning(Seq.empty).list(filterTestBucket, none, none, 1).value shouldBe None
  }
}
