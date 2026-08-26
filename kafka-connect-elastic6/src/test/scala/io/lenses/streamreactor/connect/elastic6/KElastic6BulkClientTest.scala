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
package io.lenses.streamreactor.connect.elastic6

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.sksamuel.elastic4s.bulk.BulkRequest
import com.sksamuel.elastic4s.http.Response
import com.sksamuel.elastic4s.http.bulk.BulkError
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.bulk.BulkResponseItem
import com.sksamuel.elastic4s.http.bulk.BulkResponseItems
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future
import scala.concurrent.Promise

class KElastic6BulkClientTest extends AnyWordSpec with Matchers with MockitoSugar with ArgumentMatchersSugar {

  private val sampleOps = Seq(
    InsertOp(
      index        = "myindex",
      id           = "doc1",
      json         = JsonNodeFactory.instance.objectNode().put("field", "value"),
      pipeline     = None,
      documentType = None,
    ),
  )

  "KElastic6BulkClient.bulk" should {

    "return errors=false and empty itemErrors when all items succeed" in {
      val (client, elasticClient) = setup()

      val successItem = bulkResponseItem(id = "doc1", index = "myindex", error = None, status = 201)
      val bulkResp =
        BulkResponse(took = 5L, errors = false, _items = Seq(BulkResponseItems(Some(successItem), None, None, None)))
      when(elasticClient.result).thenReturn(bulkResp)

      val result = client.bulk(sampleOps)
      result.isSuccess shouldBe true
      result.get.errors shouldBe false
      result.get.itemErrors shouldBe empty
    }

    "return errors=true and populate itemErrors in strict mode" in {
      val (client, elasticClient) = setup(strict = true)

      val err = BulkError(`type` = "mapper_parsing_exception",
                          reason     = "failed to parse field [foo]",
                          index_uuid = "abc",
                          shard      = 0,
                          index      = "myindex",
      )
      val errItem = bulkResponseItem(id = "doc1", index = "myindex", error = Some(err), status = 400)
      val bulkResp =
        BulkResponse(took = 3L, errors = true, _items = Seq(BulkResponseItems(Some(errItem), None, None, None)))
      when(elasticClient.result).thenReturn(bulkResp)

      val result = client.bulk(sampleOps)
      result.isSuccess shouldBe true
      val br = result.get
      br.errors shouldBe true
      br.itemErrors should have size 1
      br.itemErrors.head.errorType shouldBe "mapper_parsing_exception"
      br.itemErrors.head.status shouldBe 400
    }

    "return errors=false in tolerant mode even when items fail" in {
      val (client, elasticClient) = setup(strict = false)

      val err = BulkError(`type` = "mapper_parsing_exception",
                          reason     = "failed to parse field [foo]",
                          index_uuid = "abc",
                          shard      = 0,
                          index      = "myindex",
      )
      val errItem = bulkResponseItem(id = "doc1", index = "myindex", error = Some(err), status = 400)
      val bulkResp =
        BulkResponse(took = 3L, errors = true, _items = Seq(BulkResponseItems(Some(errItem), None, None, None)))
      when(elasticClient.result).thenReturn(bulkResp)

      val result = client.bulk(sampleOps)
      result.isSuccess shouldBe true
      result.get.errors shouldBe false
      result.get.itemErrors shouldBe empty
    }

    "populate 429 item errors in strict mode" in {
      val (client, elasticClient) = setup(strict = true)
      val err = BulkError(`type` = "es_rejected_execution_exception",
                          reason     = "rejected execution",
                          index_uuid = "x",
                          shard      = 0,
                          index      = "idx",
      )
      val errItem = bulkResponseItem(id = "doc1", index = "idx", error = Some(err), status = 429)
      val bulkResp =
        BulkResponse(took = 1L, errors = true, _items = Seq(BulkResponseItems(Some(errItem), None, None, None)))
      when(elasticClient.result).thenReturn(bulkResp)

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
        com.sksamuel.elastic4s.http.ElasticError("transport_error",
                                                 "could not connect",
                                                 None,
                                                 None,
                                                 None,
                                                 Seq.empty,
                                                 None,
        ),
      )

      val client = new KElastic6BulkClient(elasticClient, writeTimeoutMillis = 5000)
      val result = client.bulk(sampleOps)
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("transport error")
    }

    "time out the bulk Await in milliseconds, not seconds" in {
      val elasticClient = mock[KElasticClient]
      when(elasticClient.execute(any[BulkRequest])).thenReturn(Promise[Response[BulkResponse]]().future)

      val client    = new KElastic6BulkClient(elasticClient, writeTimeoutMillis = 200)
      val start     = System.nanoTime()
      val result    = client.bulk(sampleOps)
      val elapsedMs = (System.nanoTime() - start) / 1000000L

      result.isFailure shouldBe true
      elapsedMs should be < 5000L
    }
  }

  private def setup(strict: Boolean = true): (KElastic6BulkClient, Response[BulkResponse]) = {
    val elasticClient = mock[KElasticClient]
    val mockResp      = mock[Response[BulkResponse]]
    when(elasticClient.execute(any[BulkRequest])).thenReturn(Future.successful(mockResp))
    when(mockResp.isError).thenReturn(false)
    val client = new KElastic6BulkClient(elasticClient, writeTimeoutMillis = 5000, strictItemErrors = strict)
    (client, mockResp)
  }

  private def bulkResponseItem(
    id:     String,
    index:  String,
    error:  Option[BulkError],
    status: Int,
  ): BulkResponseItem =
    BulkResponseItem(
      itemId        = 0,
      id            = id,
      index         = index,
      `type`        = "_doc",
      version       = 1L,
      seqNo         = 0L,
      primaryTerm   = 1L,
      forcedRefresh = false,
      found         = false,
      created       = error.isEmpty,
      result        = if (error.isDefined) "error" else "created",
      status        = status,
      error         = error,
      shards        = None,
    )
}
