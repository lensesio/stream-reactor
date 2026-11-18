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

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.sink.seek.DeleteOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.DeleteOperationProcessor
import io.lenses.streamreactor.connect.cloud.common.storage.ConditionalDelete
import io.lenses.streamreactor.connect.cloud.common.storage.DeleteOutcome
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse
import software.amazon.awssdk.services.s3.model.DeletedObject
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Error
import software.amazon.awssdk.services.s3.model.S3Exception

import scala.jdk.CollectionConverters.ListHasAsScala

/**
 * Conditional deletes on S3: what each request carries (`If-Match`, per-object `ETag`) and how
 * each answer maps to an outcome. Plain Java Mockito, for the reason given in
 * `AwsS3StorageInterfaceTest`.
 */
class AwsS3ConditionalDeleteTest extends AnyFunSuite with Matchers with EitherValues {

  private val bucket = "bucket"

  private def client(): S3Client = Mockito.mock(classOf[S3Client])

  private def storage(s3: S3Client, batchDelete: Boolean): AwsS3StorageInterface =
    new AwsS3StorageInterface(Mockito.mock(classOf[ConnectorTaskId]), s3, batchDelete, None)

  private def anyDelete():  DeleteObjectRequest  = ArgumentMatchers.any(classOf[DeleteObjectRequest])
  private def anyDeletes(): DeleteObjectsRequest = ArgumentMatchers.any(classOf[DeleteObjectsRequest])

  private def preconditionFailed: S3Exception =
    S3Exception.builder().statusCode(412)
      .awsErrorDetails(AwsErrorDetails.builder().errorCode("PreconditionFailed").build())
      .build().asInstanceOf[S3Exception]

  private def noSuchKey: NoSuchKeyException =
    NoSuchKeyException.builder().statusCode(404).build().asInstanceOf[NoSuchKeyException]

  private val accessDenied: S3Exception =
    S3Exception.builder().statusCode(403)
      .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").build())
      .build().asInstanceOf[S3Exception]

  private def sentDeletes(s3: S3Client, times: Int): Seq[DeleteObjectRequest] = {
    val captor = ArgumentCaptor.forClass(classOf[DeleteObjectRequest])
    Mockito.verify(s3, Mockito.times(times)).deleteObject(captor.capture())
    captor.getAllValues.asScala.toSeq
  }

  private def sentBatch(s3: S3Client): DeleteObjectsRequest = {
    val captor = ArgumentCaptor.forClass(classOf[DeleteObjectsRequest])
    Mockito.verify(s3).deleteObjects(captor.capture())
    captor.getValue
  }

  // ---- deleteFile: the commit chain's temp delete, by path --------------------------------

  test("deleteFile deletes by path and sends no If-Match") {
    val s3 = client()
    Mockito.doReturn(DeleteObjectResponse.builder().build()).when(s3).deleteObject(anyDelete())

    storage(s3, batchDelete = false).deleteFile(bucket, "tmp/a", "\"etag-a\"") shouldBe Right(())

    val sent = sentDeletes(s3, 1).head
    (sent.bucket(), sent.key(), Option(sent.ifMatch())) shouldBe ((bucket, "tmp/a", None))
  }

  test("the commit chain's temp delete reaches S3 with no If-Match, whatever token the chain carries") {
    val s3 = client()
    Mockito.doReturn(DeleteObjectResponse.builder().build()).when(s3).deleteObject(anyDelete())
    val processor = new DeleteOperationProcessor(storage(s3, batchDelete = true))

    processor.process(DeleteOperation(bucket, ".temp-upload/t/0/uuid/data.json", "\"copy-etag\"")) shouldBe
      Right(None)

    val sent = sentDeletes(s3, 1).head
    (sent.key(), Option(sent.ifMatch())) shouldBe ((".temp-upload/t/0/uuid/data.json", None))
    Mockito.verify(s3, Mockito.never()).deleteObjects(anyDeletes())
  }

  test("deleteFile on a missing object is success, so a replayed delete step is idempotent") {
    val s3 = client()
    Mockito.doThrow(noSuchKey).when(s3).deleteObject(anyDelete())

    storage(s3, batchDelete = false).deleteFile(bucket, "tmp/a", "e") shouldBe Right(())
  }

  test("deleteFile on any other error fails with that error") {
    val s3 = client()
    Mockito.doThrow(accessDenied).when(s3).deleteObject(anyDelete())

    storage(s3, batchDelete = false).deleteFile(bucket, "tmp/a", "e").left.value.exception shouldBe accessDenied
  }

  // ---- deleteFilesIfMatch, batch delete off ---------------------------------------------

  test("deleteFilesIfMatch without batch delete sends one If-Match delete per object and maps each answer") {
    val s3 = client()
    Mockito.doReturn(DeleteObjectResponse.builder().build())
      .doThrow(preconditionFailed)
      .doThrow(noSuchKey)
      .doThrow(accessDenied)
      .when(s3).deleteObject(anyDelete())

    val outcomes = storage(s3, batchDelete = false).deleteFilesIfMatch(
      bucket,
      Seq(ConditionalDelete("a", "ea"),
          ConditionalDelete("b", "eb"),
          ConditionalDelete("c", "ec"),
          ConditionalDelete("d", "ed"),
      ),
    )

    outcomes.take(3) shouldBe Seq(DeleteOutcome.Deleted, DeleteOutcome.PreconditionFailed, DeleteOutcome.NotFound)
    outcomes(3) shouldBe DeleteOutcome.Failed(outcomes(3).asInstanceOf[DeleteOutcome.Failed].error)
    outcomes(3).asInstanceOf[DeleteOutcome.Failed].error.exception shouldBe accessDenied
    sentDeletes(s3, 4).map(r => r.key() -> r.ifMatch()) shouldBe Seq("a" -> "ea", "b" -> "eb", "c" -> "ec", "d" -> "ed")
    Mockito.verify(s3, Mockito.never()).deleteObjects(anyDeletes())
  }

  // ---- deleteFilesIfMatch, batch delete on ----------------------------------------------

  test("deleteFilesIfMatch with batch delete sends one DeleteObjects carrying each object's eTag") {
    val s3 = client()
    Mockito.doReturn(
      DeleteObjectsResponse.builder()
        .deleted(DeletedObject.builder().key("a").build())
        .errors(
          S3Error.builder().key("b").code("PreconditionFailed").message("At least one precondition failed").build(),
          S3Error.builder().key("c").code("NoSuchKey").message("The specified key does not exist.").build(),
          S3Error.builder().key("d").code("AccessDenied").message("Access Denied").build(),
        )
        .build(),
    ).when(s3).deleteObjects(anyDeletes())

    val outcomes = storage(s3, batchDelete = true).deleteFilesIfMatch(
      bucket,
      Seq(
        ConditionalDelete("d", "ed"),
        ConditionalDelete("c", "ec"),
        ConditionalDelete("b", "eb"),
        ConditionalDelete("a", "ea"),
        ConditionalDelete("e", "ee"),
      ),
    )

    outcomes(0) shouldBe a[DeleteOutcome.Failed]
    outcomes(0).asInstanceOf[DeleteOutcome.Failed].error.message() should include("AccessDenied")
    outcomes.slice(1, 4) shouldBe Seq(DeleteOutcome.NotFound, DeleteOutcome.PreconditionFailed, DeleteOutcome.Deleted)
    // Reported in neither list: nothing is known, so it is a failure (the drain retries it).
    outcomes(4) shouldBe a[DeleteOutcome.Failed]

    val sent = sentBatch(s3)
    sent.bucket() shouldBe bucket
    sent.delete().quiet() shouldBe false
    sent.delete().objects().asScala.map(o => o.key() -> o.eTag()) shouldBe
      Seq("d" -> "ed", "c" -> "ec", "b" -> "eb", "a" -> "ea", "e" -> "ee")
    Mockito.verify(s3, Mockito.never()).deleteObject(anyDelete())
  }

  test("deleteFilesIfMatch with batch delete reports a failed request as a failure for every object") {
    val s3 = client()
    Mockito.doThrow(accessDenied).when(s3).deleteObjects(anyDeletes())

    val outcomes = storage(s3, batchDelete = true)
      .deleteFilesIfMatch(bucket, Seq(ConditionalDelete("a", "ea"), ConditionalDelete("b", "eb")))

    outcomes should have size 2
    all(outcomes) shouldBe a[DeleteOutcome.Failed]
  }

  test("deleteFilesIfMatch with batch delete falls back to per-object deletes when a key is given twice") {
    val s3 = client()
    Mockito.doThrow(preconditionFailed).doReturn(DeleteObjectResponse.builder().build())
      .when(s3).deleteObject(anyDelete())

    storage(s3, batchDelete = true).deleteFilesIfMatch(
      bucket,
      Seq(ConditionalDelete("a", "stale"), ConditionalDelete("a", "current")),
    ) shouldBe Seq(DeleteOutcome.PreconditionFailed, DeleteOutcome.Deleted)

    sentDeletes(s3, 2).map(_.ifMatch()) shouldBe Seq("stale", "current")
    Mockito.verify(s3, Mockito.never()).deleteObjects(anyDeletes())
  }

  test("deleteFilesIfMatch of nothing sends nothing") {
    val s3 = client()
    storage(s3, batchDelete = true).deleteFilesIfMatch(bucket, Seq.empty) shouldBe empty
    Mockito.verifyNoInteractions(s3)
  }

  // ---- deleteFiles (unconditional) --------------------------------------------------------

  test("deleteFiles with batch delete fails when DeleteObjects reports a per-key error other than NoSuchKey") {
    val s3 = client()
    Mockito.doReturn(
      DeleteObjectsResponse.builder()
        .deleted(DeletedObject.builder().key("a").build())
        .errors(S3Error.builder().key("b").code("AccessDenied").message("Access Denied").build())
        .build(),
    ).when(s3).deleteObjects(anyDeletes())

    val error = storage(s3, batchDelete = true).deleteFiles(bucket, Seq("a", "b")).left.value
    error.message() should include("AccessDenied")
    error.fileName shouldBe "b"
    sentBatch(s3).delete().objects().asScala.map(o => Option(o.eTag())) shouldBe Seq(None, None)
  }

  test("deleteFiles with batch delete treats a NoSuchKey entry as deleted") {
    val s3 = client()
    Mockito.doReturn(
      DeleteObjectsResponse.builder()
        .errors(S3Error.builder().key("b").code("NoSuchKey").message("gone").build())
        .build(),
    ).when(s3).deleteObjects(anyDeletes())

    storage(s3, batchDelete = true).deleteFiles(bucket, Seq("a", "b")) shouldBe Right(())
  }
}
