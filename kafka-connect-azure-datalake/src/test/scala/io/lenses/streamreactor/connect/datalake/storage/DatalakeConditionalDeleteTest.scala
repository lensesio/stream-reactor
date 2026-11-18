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
package io.lenses.streamreactor.connect.datalake.storage

import com.azure.core.http.HttpHeaders
import com.azure.core.http.HttpMethod
import com.azure.core.http.HttpRequest
import com.azure.core.http.HttpResponse
import com.azure.core.http.rest.Response
import com.azure.core.http.rest.SimpleResponse
import com.azure.storage.file.datalake.DataLakeFileClient
import com.azure.storage.file.datalake.DataLakeFileSystemClient
import com.azure.storage.file.datalake.DataLakeServiceClient
import com.azure.storage.file.datalake.models.DataLakeStorageException
import com.azure.storage.file.datalake.options.DataLakePathDeleteOptions
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.storage.ConditionalDelete
import io.lenses.streamreactor.connect.cloud.common.storage.DeleteOutcome
import io.lenses.streamreactor.connect.cloud.common.storage.DeletePreconditionFailedException
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

import java.nio.ByteBuffer
import java.nio.charset.Charset
import scala.collection.mutable

/**
 * Conditional deletes on ADLS: one `If-Match` path delete per object, and each answer mapped to an
 * outcome. A 412 is checked against the path's existence, because a missing path under `If-Match`
 * answers 412 on Azurite and the real service has not been verified.
 */
class DatalakeConditionalDeleteTest extends AnyFunSuite with Matchers with EitherValues {

  private val bucket = "bucket"

  private final class TestHttpResponse(status: Int)
      extends HttpResponse(new HttpRequest(HttpMethod.DELETE, "https://example.com")) {
    override def getStatusCode: Int         = status
    override def getHeaders:    HttpHeaders = new HttpHeaders()
    override def getHeaderValue(name: String): String = null
    override def getBodyAsByteArray: Mono[Array[Byte]] = Mono.just(Array.emptyByteArray)
    override def getBody:            Flux[ByteBuffer]  = Flux.empty()
    override def getBodyAsString(charset: Charset): Mono[String] = Mono.just("")
    override def getBodyAsString(): Mono[String] = Mono.just("")
  }

  private def status(code: Int): DataLakeStorageException =
    new DataLakeStorageException(s"status $code", new TestHttpResponse(code), null)

  /**
   * A file-system client whose `If-Match` deletes answer per path from `deletes`, and whose
   * existence checks answer from `exists`. Records every delete as (path, If-Match).
   */
  private final class Fixture(deletes: Map[String, () => Boolean], exists: Map[String, () => Boolean] = Map.empty) {
    val sentDeletes:  mutable.Buffer[(String, String)] = mutable.Buffer.empty
    val existsChecks: mutable.Buffer[String]           = mutable.Buffer.empty

    private val fs = Mockito.mock(classOf[DataLakeFileSystemClient])
    Mockito.when(
      fs.deleteFileIfExistsWithResponse(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.any(classOf[DataLakePathDeleteOptions]),
        ArgumentMatchers.isNull[java.time.Duration](),
        ArgumentMatchers.any(classOf[com.azure.core.util.Context]),
      ),
    ).thenAnswer { (inv: InvocationOnMock) =>
      val path    = inv.getArgument[String](0)
      val options = inv.getArgument[DataLakePathDeleteOptions](1)
      sentDeletes += path -> options.getRequestConditions.getIfMatch
      new SimpleResponse[java.lang.Boolean](null, 200, null, Boolean.box(deletes(path)())): Response[java.lang.Boolean]
    }
    Mockito.when(fs.getFileClient(ArgumentMatchers.anyString())).thenAnswer { (inv: InvocationOnMock) =>
      val path = inv.getArgument[String](0)
      val file = Mockito.mock(classOf[DataLakeFileClient])
      Mockito.when(file.exists()).thenAnswer { (_: InvocationOnMock) =>
        existsChecks += path
        Boolean.box(exists(path)())
      }
      file
    }

    private val service = Mockito.mock(classOf[DataLakeServiceClient])
    Mockito.when(service.getFileSystemClient(bucket)).thenReturn(fs)

    val storage = new DatalakeStorageInterface(ConnectorTaskId("connector", 1, 1), service)
  }

  private def throws(code: Int): () => Boolean = () => throw status(code)

  test("deleteFilesIfMatch sends one If-Match delete per object and maps each answer") {
    val f = new Fixture(
      deletes = Map(
        "a" -> (() => true),
        "b" -> (() => false),
        "c" -> throws(412),
        "d" -> throws(412),
        "e" -> throws(404),
        "g" -> throws(403),
      ),
      exists = Map("c" -> (() => true), "d" -> (() => false)),
    )

    val outcomes = f.storage.deleteFilesIfMatch(
      bucket,
      Seq("a", "b", "c", "d", "e", "g").map(p => ConditionalDelete(p, s"etag-$p")),
    )

    outcomes.take(5) shouldBe Seq(
      DeleteOutcome.Deleted,
      DeleteOutcome.NotFound,
      DeleteOutcome.PreconditionFailed, // 412, still there: another version
      DeleteOutcome.NotFound,           // 412 on a missing path
      DeleteOutcome.NotFound,
    )
    outcomes(5) shouldBe a[DeleteOutcome.Failed]
    f.sentDeletes.toSeq shouldBe Seq("a", "b", "c", "d", "e", "g").map(p => p -> s"etag-$p")
    // Only an ambiguous 412 costs an existence check.
    f.existsChecks.toSeq shouldBe Seq("c", "d")
  }

  test("a 412 whose existence check fails is a failure, not a decision") {
    val f        = new Fixture(deletes = Map("a" -> throws(412)), exists = Map("a" -> throws(500)))
    val outcomes = f.storage.deleteFilesIfMatch(bucket, Seq(ConditionalDelete("a", "e")))
    outcomes.head shouldBe a[DeleteOutcome.Failed]
  }

  test("deleteFile is conditional: success on a match or a missing path, a precondition failure otherwise") {
    val f = new Fixture(
      deletes = Map("a" -> (() => true), "b" -> (() => false), "c" -> throws(412)),
      exists  = Map("c" -> (() => true)),
    )

    f.storage.deleteFile(bucket, "a", "ea") shouldBe Right(())
    f.storage.deleteFile(bucket, "b", "eb") shouldBe Right(())
    f.storage.deleteFile(bucket, "c", "ec").left.value.exception shouldBe a[DeletePreconditionFailedException]
    f.sentDeletes.toSeq shouldBe Seq("a" -> "ea", "b" -> "eb", "c" -> "ec")
  }
}
