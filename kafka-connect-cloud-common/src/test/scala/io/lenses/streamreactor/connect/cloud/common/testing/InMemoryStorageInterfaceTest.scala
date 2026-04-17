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
package io.lenses.streamreactor.connect.cloud.common.testing

import io.circe.generic.semiauto._
import io.circe.Decoder
import io.circe.Encoder
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.sink.seek.NoOverwriteExistingObject
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectWithETag
import io.lenses.streamreactor.connect.cloud.common.storage.FileCreateError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMoveError
import io.lenses.streamreactor.connect.cloud.common.storage.FileNotFoundError
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface._
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

/**
 * Smoke tests for [[InMemoryStorageInterface]]: cover the few semantic edges that are
 * crucial for exactly-once scenarios so a scenario failure can never be misattributed to
 * the fake. We test the conditional-write rules (NoOverwrite, eTag CAS), the mvFile
 * idempotency rule (post-fix S3 semantics), and one or two crash-injection hooks end to
 * end.
 */
class InMemoryStorageInterfaceTest extends AnyFunSuiteLike with Matchers with EitherValues {

  private val Bucket = "test-bucket"

  private case class Sample(name: String, value: Int)
  private implicit val sampleEncoder: Encoder[Sample] = deriveEncoder
  private implicit val sampleDecoder: Decoder[Sample] = deriveDecoder

  private def newFake(): InMemoryStorageInterface = new InMemoryStorageInterface()

  private def writeTempFile(contents: String): File = {
    val f = Files.createTempFile("eo-fake-", ".bin").toFile
    Files.write(f.toPath, contents.getBytes(StandardCharsets.UTF_8))
    f.deleteOnExit()
    f
  }

  test("uploadFile persists bytes and returns a fresh eTag; pathExists reflects that") {
    val fake = newFake()
    val src  = writeTempFile("hello")
    val tag  = fake.uploadFile(UploadableFile(src), Bucket, "k1").value
    tag should not be empty
    fake.pathExists(Bucket, "k1").value shouldBe true
    fake.getBlobAsString(Bucket, "k1").value shouldBe "hello"
  }

  test("getBlobAsObject on a missing key returns FileNotFoundError") {
    val fake = newFake()
    fake.getBlobAsObject[Sample](Bucket, "missing").left.value shouldBe a[FileNotFoundError]
  }

  test("writeBlobToFile with NoOverwriteExistingObject succeeds when absent and fails when present") {
    val fake = newFake()
    val ok   = fake.writeBlobToFile(Bucket, "lock", NoOverwriteExistingObject(Sample("a", 1))).value
    ok.eTag should not be empty

    fake.writeBlobToFile(Bucket, "lock", NoOverwriteExistingObject(Sample("b", 2)))
      .left.value shouldBe a[FileCreateError]

    // Original survived.
    fake.getBlobAsObject[Sample](Bucket, "lock").value.wrappedObject shouldBe Sample("a", 1)
  }

  test("writeBlobToFile with ObjectWithETag enforces compare-and-swap") {
    val fake    = newFake()
    val created = fake.writeBlobToFile(Bucket, "lock", NoOverwriteExistingObject(Sample("a", 1))).value

    // Stale eTag fails.
    fake.writeBlobToFile(Bucket, "lock", ObjectWithETag(Sample("c", 3), "stale-etag"))
      .left.value shouldBe a[FileCreateError]

    // Correct eTag succeeds and rotates the eTag.
    val updated = fake.writeBlobToFile(Bucket, "lock", ObjectWithETag(Sample("c", 3), created.eTag)).value
    updated.eTag should not equal created.eTag
    fake.getBlobAsObject[Sample](Bucket, "lock").value.wrappedObject shouldBe Sample("c", 3)
  }

  test("mvFile is idempotent when source missing but destination present") {
    val fake = newFake()
    val src  = writeTempFile("x")
    val _    = fake.uploadFile(UploadableFile(src), Bucket, "moved").value
    fake.mvFile(Bucket, "missing-source", Bucket, "moved", None) shouldBe Right(())
  }

  test("mvFile fails loudly when both source and destination are missing") {
    val fake = newFake()
    fake.mvFile(Bucket, "absent-src", Bucket, "absent-dst", None).left.value shouldBe a[FileMoveError]
  }

  test("mvFile rejects an eTag mismatch on the source") {
    val fake = newFake()
    val src  = writeTempFile("payload")
    val tag  = fake.uploadFile(UploadableFile(src), Bucket, "src").value
    val _    = tag

    fake.mvFile(Bucket, "src", Bucket, "dst", Some("not-the-real-etag")).left.value shouldBe a[FileMoveError]
    fake.pathExists(Bucket, "dst").value shouldBe false
  }

  test("FailWriteAt hook fires once and is then consumed") {
    val fake = newFake().arm(FailWriteAt(Bucket, "blocked"))
    val src  = writeTempFile("hi")
    fake.uploadFile(UploadableFile(src), Bucket, "blocked").left.value shouldBe a[FileCreateError]
    // Hook is gone after first match.
    fake.uploadFile(UploadableFile(src), Bucket, "blocked").value should not be empty
  }

  test("CorruptETag returns a wrong eTag to the caller while persisting the bytes correctly") {
    val fake     = newFake().arm(CorruptETag(Bucket, "lock"))
    val response = fake.writeBlobToFile(Bucket, "lock", NoOverwriteExistingObject(Sample("a", 1))).value
    val stored   = fake.getBlobAsObject[Sample](Bucket, "lock").value
    stored.wrappedObject shouldBe Sample("a", 1)
    // Caller's cached eTag does NOT match what's stored.
    stored.eTag should not equal response.eTag
  }

  test("DropUploadAfterCount drops the (n+1)th matching upload only") {
    val fake = newFake().arm(DropUploadAfterCount(Bucket, threshold = 2))
    val src  = writeTempFile("d")
    val _    = fake.uploadFile(UploadableFile(src), Bucket, "k1").value
    val _    = fake.uploadFile(UploadableFile(src), Bucket, "k2").value
    val _    = fake.uploadFile(UploadableFile(src), Bucket, "k3").value // dropped
    fake.pathExists(Bucket, "k1").value shouldBe true
    fake.pathExists(Bucket, "k2").value shouldBe true
    fake.pathExists(Bucket, "k3").value shouldBe false
    // Hook consumed: a 4th upload persists normally.
    val _ = fake.uploadFile(UploadableFile(src), Bucket, "k4").value
    fake.pathExists(Bucket, "k4").value shouldBe true
  }

  test("deleteFile is a no-op on a missing key") {
    val fake = newFake()
    fake.deleteFile(Bucket, "ghost", "no-etag") shouldBe Right(())
  }

  test("listKeysRecursive returns only the keys under the prefix") {
    val fake = newFake()
    val src  = writeTempFile("l")
    val _    = fake.uploadFile(UploadableFile(src), Bucket, "a/x").value
    val _    = fake.uploadFile(UploadableFile(src), Bucket, "a/y").value
    val _    = fake.uploadFile(UploadableFile(src), Bucket, "b/z").value
    val resp = fake.listKeysRecursive(Bucket, Some("a/")).value.getOrElse(fail("expected non-empty list"))
    resp.files.toSet shouldBe Set("a/x", "a/y")
  }
}
