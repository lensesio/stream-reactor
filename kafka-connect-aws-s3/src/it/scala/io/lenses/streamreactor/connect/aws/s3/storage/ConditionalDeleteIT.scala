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

import io.lenses.streamreactor.connect.aws.s3.auth.AwsS3ClientCreator
import io.lenses.streamreactor.connect.aws.s3.config.S3ConnectionConfig
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.storage.ConditionalDelete
import io.lenses.streamreactor.connect.cloud.common.storage.DeleteOutcome
import io.lenses.streamreactor.connect.testcontainers.FlociContainer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest

/**
 * Conditional deletes through the real `AwsS3StorageInterface` against an S3 emulator that
 * honours `If-Match` on `DeleteObject` and the per-object `ETag` of `DeleteObjects` as AWS does:
 * a match deletes, another version is refused (412 / `PreconditionFailed`), a missing key is
 * `NoSuchKey`. Both the single-object path (batch delete off) and `DeleteObjects` (on).
 */
class ConditionalDeleteIT extends AnyFunSuite with Matchers with EitherValues with BeforeAndAfterAll {

  private val container = new FlociContainer()
  private val bucket    = "conditional-delete-it"

  private var client: S3Client = _

  override def beforeAll(): Unit = {
    container.start()
    client = AwsS3ClientCreator.make(
      S3ConnectionConfig(
        region          = Some(FlociContainer.Region),
        accessKey       = Some(FlociContainer.AccessKey),
        secretKey       = Some(FlociContainer.SecretKey),
        authMode        = AuthMode.Credentials,
        customEndpoint  = Some(container.getEndpointUrl.toString),
        pathStyleAccess = Some(true),
      ),
    ).fold(throw _, identity)
    val _ = client.createBucket(CreateBucketRequest.builder().bucket(bucket).build())
  }

  override def afterAll(): Unit =
    try Option(client).foreach(_.close())
    finally container.stop()

  private def storage(batchDelete: Boolean) =
    new AwsS3StorageInterface(ConnectorTaskId("conditional-delete-it", 1, 0), client, batchDelete, None)

  /** Writes `key` and returns the eTag the store reports for it. */
  private def put(key: String, content: String): String = {
    val si = storage(batchDelete = false)
    si.writeStringToFile(bucket, key, UploadableString(content)).value
    si.getBlobAsStringAndEtag(bucket, key).value._2
  }

  private def exists(key: String): Boolean = storage(batchDelete = false).pathExists(bucket, key).value

  Seq(false, true).foreach { batchDelete =>
    val mode = if (batchDelete) "DeleteObjects" else "DeleteObject"

    test(s"$mode: a match deletes, another version is refused and kept, a missing key is not found") {
      val prefix  = s"$mode/outcomes"
      val matched = put(s"$prefix/matched", "v1")
      val _       = put(s"$prefix/changed", "v1")
      val stale   = storage(batchDelete).getBlobAsStringAndEtag(bucket, s"$prefix/changed").value._2
      val _       = put(s"$prefix/changed", "v2") // rewritten after the token was taken

      val outcomes = storage(batchDelete).deleteFilesIfMatch(
        bucket,
        Seq(
          ConditionalDelete(s"$prefix/matched", matched),
          ConditionalDelete(s"$prefix/changed", stale),
          ConditionalDelete(s"$prefix/missing", matched),
        ),
      )

      outcomes shouldBe Seq(DeleteOutcome.Deleted, DeleteOutcome.PreconditionFailed, DeleteOutcome.NotFound)
      exists(s"$prefix/matched") shouldBe false
      exists(s"$prefix/changed") shouldBe true
      storage(batchDelete).getBlobAsString(bucket, s"$prefix/changed").value shouldBe "v2"
    }

    test(s"$mode: deleteFile deletes by path whatever the token, and a missing object is success") {
      val key = s"$mode/single"
      val _   = put(key, "v1")
      val si  = storage(batchDelete)

      si.deleteFile(bucket, key, "\"0123456789abcdef0123456789abcdef\"") shouldBe Right(())
      exists(key) shouldBe false
      si.deleteFile(bucket, key, "\"0123456789abcdef0123456789abcdef\"") shouldBe Right(())
    }
  }

  test("deleteFiles stays unconditional, and a batch with a missing key succeeds") {
    val _ = put("unconditional/a", "a")
    storage(batchDelete = true).deleteFiles(bucket, Seq("unconditional/a", "unconditional/missing")) shouldBe Right(())
    exists("unconditional/a") shouldBe false
  }
}
