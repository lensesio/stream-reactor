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
package io.lenses.streamreactor.connect.elastic7

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.bulk.BulkError
import com.sksamuel.elastic4s.requests.bulk.BulkRequest
import com.sksamuel.elastic4s.requests.bulk.BulkResponse
import com.sksamuel.elastic4s.requests.bulk.BulkResponseItem
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future
import scala.concurrent.Promise

class KElasticBulkClientTest extends AnyWordSpec with Matchers with MockitoSugar with ArgumentMatchersSugar {

  private val sampleOps = Seq(
    InsertOp(
      index        = "myindex",
      id           = "doc1",
      json         = JsonNodeFactory.instance.objectNode().put("field", "value"),
      pipeline     = None,
      documentType = None,
    ),
  )

  "KElasticBulkClient.bulk" should {

    "return errors=false when all items succeed" in {
      val (client, _) = setup(strict = true)
      val result      = client.bulk(sampleOps)
      result.isSuccess shouldBe true
      result.get.errors shouldBe false
      result.get.itemErrors shouldBe empty
    }

    "return errors=true and populate itemErrors in strict mode" in {
      val err         = bulkError("mapper_parsing_exception", "failed to parse field [foo]")
      val item        = bulkItem(id = "doc1", index = "myindex", status = 400, error = Some(err))
      val (client, _) = setup(strict = true, items = Seq(item))

      val result = client.bulk(sampleOps)
      result.isSuccess shouldBe true
      val br = result.get
      br.errors shouldBe true
      br.itemErrors should have size 1
      br.itemErrors.head.errorType shouldBe "mapper_parsing_exception"
      br.itemErrors.head.status shouldBe 400
      br.itemErrors.head.reason should include("failed to parse")
    }

    "return errors=false in tolerant mode even when items fail" in {
      val err         = bulkError("mapper_parsing_exception", "failed to parse field [foo]")
      val item        = bulkItem(id = "doc1", index = "myindex", status = 400, error = Some(err))
      val (client, _) = setup(strict = false, items = Seq(item))

      val result = client.bulk(sampleOps)
      result.isSuccess shouldBe true
      result.get.errors shouldBe false
      result.get.itemErrors shouldBe empty
    }

    "populate 429 / es_rejected_execution_exception on the item error" in {
      val err         = bulkError("es_rejected_execution_exception", "rejected execution of org.elasticsearch.transport")
      val item        = bulkItem(id = "doc1", index = "myindex", status = 429, error = Some(err))
      val (client, _) = setup(strict = true, items = Seq(item))

      val br = client.bulk(sampleOps).get
      br.errors shouldBe true
      br.itemErrors.head.status shouldBe 429
      br.itemErrors.head.errorType shouldBe "es_rejected_execution_exception"
    }

    "surface a transport-level error as a Failure" in {
      val elasticClient = mock[KElasticClient]
      val mockResp      = mock[Response[BulkResponse]]
      when(elasticClient.execute(any[BulkRequest])).thenReturn(Future.successful(mockResp))
      when(mockResp.isError).thenReturn(true)
      when(mockResp.error).thenReturn(
        com.sksamuel.elastic4s.ElasticError(
          `type`       = "transport_error",
          reason       = "could not connect",
          indexUuid    = None,
          index        = None,
          shard        = None,
          rootCause    = Seq.empty,
          causedBy     = None,
          phase        = None,
          grouped      = None,
          failedShards = Seq.empty,
        ),
      )

      val client = new KElasticBulkClient(elasticClient, writeTimeoutMillis = 5000, strictItemErrors = true)
      val result = client.bulk(sampleOps)
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("transport error")
    }

    "time out the bulk Await in milliseconds, not seconds" in {
      val elasticClient = mock[KElasticClient]
      when(elasticClient.execute(any[BulkRequest])).thenReturn(Promise[Response[BulkResponse]]().future)

      val client    = new KElasticBulkClient(elasticClient, writeTimeoutMillis = 200, strictItemErrors = true)
      val start     = System.nanoTime()
      val result    = client.bulk(sampleOps)
      val elapsedMs = (System.nanoTime() - start) / 1000000L

      result.isFailure shouldBe true
      elapsedMs should be < 5000L
    }
  }

  private def setup(
    strict: Boolean,
    items:  Seq[BulkResponseItem] = Seq.empty,
  ): (KElasticBulkClient, Response[BulkResponse]) = {
    val elasticClient = mock[KElasticClient]
    val mockResp      = mock[Response[BulkResponse]]
    val bulkResult    = mock[BulkResponse]
    when(elasticClient.execute(any[BulkRequest])).thenReturn(Future.successful(mockResp))
    when(mockResp.isError).thenReturn(false)
    when(mockResp.result).thenReturn(bulkResult)
    when(bulkResult.took).thenReturn(1L)
    when(bulkResult.items).thenReturn(items)
    val client = new KElasticBulkClient(elasticClient, writeTimeoutMillis = 5000, strictItemErrors = strict)
    (client, mockResp)
  }

  private def bulkError(`type`: String, reason: String): BulkError = {
    val err = mock[BulkError]
    when(err.`type`).thenReturn(`type`)
    when(err.reason).thenReturn(reason)
    err
  }

  private def bulkItem(id: String, index: String, status: Int, error: Option[BulkError]): BulkResponseItem = {
    val item = mock[BulkResponseItem]
    when(item.id).thenReturn(id)
    when(item.index).thenReturn(index)
    when(item.status).thenReturn(status)
    when(item.error).thenReturn(error)
    item
  }
}
