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
package io.lenses.streamreactor.connect.gcp.storage

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.BlobSourceOption
import com.google.cloud.storage.StorageException
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.storage.ConditionalDelete
import io.lenses.streamreactor.connect.cloud.common.storage.DeleteOutcome
import io.lenses.streamreactor.connect.cloud.common.storage.DeletePreconditionFailedException
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageStorageInterface
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

/**
 * Conditional deletes on GCS: one `delete(blobId, ifGenerationMatch)` per object, with the token
 * read as the generation, and each answer mapped to an outcome. The client is a recording stub:
 * every call is kept, and each conditional delete answers from `answers` by object name.
 */
class GCPStorageConditionalDeleteTest extends AnyFunSuite with Matchers with EitherValues {

  private val bucket = "bucket"

  /** A `Storage` whose conditional deletes answer per object, recording (method, name, options). */
  private final class RecordingStorage(answers: Map[String, () => Boolean]) {
    val calls: mutable.Buffer[(String, Seq[Any])] = mutable.Buffer.empty
    val client: Storage = Mockito.mock(
      classOf[Storage],
      (invocation: InvocationOnMock) => {
        val args = invocation.getArguments.toSeq.flatMap {
          case options: Array[_] => options.toSeq
          case other => Seq(other)
        }
        calls += invocation.getMethod.getName -> args
        args.headOption match {
          case Some(id: BlobId) if invocation.getMethod.getName == "delete" && args.size == 2 =>
            Boolean.box(answers(id.getName)())
          case _ => throw new UnsupportedOperationException(invocation.getMethod.toString)
        }
      },
    )
  }

  private def storage(client: Storage) =
    new GCPStorageStorageInterface(ConnectorTaskId("connector", 1, 1), client, Option.empty)

  private def fails(code: Int): () => Boolean = () => throw new StorageException(code, s"status $code")

  test("deleteFilesIfMatch deletes each object under its own generation and maps each answer") {
    val stub = new RecordingStorage(
      Map("a" -> (() => true), "b" -> fails(412), "c" -> (() => false), "d" -> fails(404), "e" -> fails(403)),
    )

    val outcomes = storage(stub.client).deleteFilesIfMatch(
      bucket,
      Seq("a" -> "1", "b" -> "2", "c" -> "3", "d" -> "4", "e" -> "5").map { case (p, g) => ConditionalDelete(p, g) },
    )

    outcomes.take(4) shouldBe Seq(
      DeleteOutcome.Deleted,
      DeleteOutcome.PreconditionFailed,
      DeleteOutcome.NotFound,
      DeleteOutcome.NotFound,
    )
    outcomes(4) shouldBe a[DeleteOutcome.Failed]
    outcomes(4).asInstanceOf[DeleteOutcome.Failed].error.exception.asInstanceOf[StorageException].getCode shouldBe 403
    // One conditional delete per object, each carrying its own generation; nothing else was sent.
    stub.calls.toSeq shouldBe Seq("a" -> 1L, "b" -> 2L, "c" -> 3L, "d" -> 4L, "e" -> 5L).map {
      case (p, g) => "delete" -> Seq(BlobId.of(bucket, p), BlobSourceOption.generationMatch(g))
    }
  }

  test("conditionalDeleteIsBatched is false: each object is its own request") {
    val stub = new RecordingStorage(Map("a" -> (() => true), "b" -> (() => true)))
    val si   = storage(stub.client)
    si.conditionalDeleteIsBatched shouldBe false
    si.deleteFilesIfMatch(bucket, Seq(ConditionalDelete("a", "1"), ConditionalDelete("b", "2"))) shouldBe
      Seq(DeleteOutcome.Deleted, DeleteOutcome.Deleted)
    stub.calls.map(_._1) shouldBe Seq("delete", "delete")
  }

  test("a token that is not a generation fails that object without a request") {
    val stub     = new RecordingStorage(Map.empty)
    val outcomes = storage(stub.client).deleteFilesIfMatch(bucket, Seq(ConditionalDelete("a", "\"etag\"")))
    outcomes.head shouldBe a[DeleteOutcome.Failed]
    stub.calls shouldBe empty
  }

  test("deleteFile is conditional: success on a match or a missing blob, a precondition failure otherwise") {
    val stub = new RecordingStorage(Map("a" -> (() => true), "b" -> (() => false), "c" -> fails(412)))
    val si   = storage(stub.client)

    si.deleteFile(bucket, "a", "1") shouldBe Right(())
    si.deleteFile(bucket, "b", "2") shouldBe Right(())
    si.deleteFile(bucket, "c", "3").left.value.exception shouldBe a[DeletePreconditionFailedException]
    stub.calls.map(_._2.last) shouldBe Seq(1L, 2L, 3L).map(BlobSourceOption.generationMatch)
  }
}
