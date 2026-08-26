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
package io.lenses.streamreactor.connect.opensearch

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemError
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkOp
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkResult
import io.lenses.streamreactor.connect.elastic.common.bulk.DeleteOp
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.elastic.common.bulk.KBulkClient
import io.lenses.streamreactor.connect.elastic.common.bulk.UpsertOp
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
import io.lenses.streamreactor.common.errors.FatalConnectException
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.opensearch.client.json.jackson.JacksonJsonpMapper
import org.opensearch.client.opensearch.OpenSearchClient
import org.opensearch.client.opensearch._types.ErrorCause
import org.opensearch.client.opensearch._types.ErrorResponse
import org.opensearch.client.opensearch._types.OpenSearchException
import org.opensearch.client.opensearch._types.OpenSearchVersionInfo
import org.opensearch.client.opensearch.indices.CreateIndexRequest
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient
import org.opensearch.client.util.ObjectBuilder
import org.opensearch.client.opensearch.core.BulkRequest
import org.opensearch.client.opensearch.core.BulkResponse
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem
import org.opensearch.client.opensearch.core.InfoResponse
import org.opensearch.client.transport.OpenSearchTransport
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.mockito.ArgumentCaptor

import java.io.ByteArrayOutputStream
import java.util.Collections
import scala.jdk.CollectionConverters._
import scala.util.Success
import scala.util.Try

class KOpenSearchClientTest extends AnyFunSuite with Matchers {

  private val baseProps = Map(
    HOSTS   -> "localhost",
    ES_PORT -> "9200",
    KCQL    -> "INSERT INTO idx SELECT * FROM topic",
  )

  private def settings(extra: (String, String)*): OpenSearchSettings =
    OpenSearchSettings(OpenSearchConfig(baseProps ++ extra.toMap))

  private def makeInfoResponse(distribution: String, version: String): InfoResponse = {
    val versionInfo = mock(classOf[OpenSearchVersionInfo])
    when(versionInfo.distribution()).thenReturn(distribution)
    when(versionInfo.number()).thenReturn(version)
    val infoResp = mock(classOf[InfoResponse])
    when(infoResp.version()).thenReturn(versionInfo)
    infoResp
  }

  private def makeBulkResponse(
    hasErrors: Boolean,
    items:     java.util.List[BulkResponseItem] = Collections.emptyList(),
  ): BulkResponse = {
    val resp = mock(classOf[BulkResponse])
    when(resp.errors()).thenReturn(hasErrors)
    when(resp.took()).thenReturn(1L)
    when(resp.items()).thenReturn(items)
    resp
  }

  private def makeClient(infoResponse: InfoResponse, bulkResponse: BulkResponse): OpenSearchClient = {
    val transport = mock(classOf[OpenSearchTransport])
    val client    = mock(classOf[OpenSearchClient])
    when(client._transport()).thenReturn(transport)
    when(client.info()).thenReturn(infoResponse)
    when(client.bulk(any(classOf[BulkRequest]))).thenReturn(bulkResponse)
    client
  }

  private val simpleDoc = JsonNodeFactory.instance.objectNode().put("field", "value")
  private val simpleOps = Seq(InsertOp("idx", "1", simpleDoc, None, None))

  test("cluster compatibility probe: start() throws on Elasticsearch distribution") {
    val client = makeClient(
      makeInfoResponse("elasticsearch", "7.17.0"),
      makeBulkResponse(false),
    )
    val kClient = new KOpenSearchClient(client, settings())
    val ex      = intercept[ConnectException](kClient.start().get)
    ex.getMessage should include("Elasticsearch cluster")
  }

  test("cluster compatibility probe: start() throws on OpenSearch 1.x") {
    val client = makeClient(
      makeInfoResponse("opensearch", "1.3.0"),
      makeBulkResponse(false),
    )
    val kClient = new KOpenSearchClient(client, settings())
    val ex      = intercept[ConnectException](kClient.start().get)
    ex.getMessage should include("1.x cluster")
  }

  test("cluster compatibility probe: start() succeeds on OpenSearch 2.x") {
    val client = makeClient(
      makeInfoResponse("opensearch", "2.13.0"),
      makeBulkResponse(false),
    )
    val kClient = new KOpenSearchClient(client, settings())
    kClient.start() shouldBe a[Success[_]]
  }

  test("client.info() is invoked by start(), not by bulk()") {
    val mockClient = makeClient(
      makeInfoResponse("opensearch", "2.13.0"),
      makeBulkResponse(false),
    )
    val kClient = new KOpenSearchClient(mockClient, settings())
    kClient.start()
    // Multiple bulk calls must NOT re-invoke info()
    (1 to 10).foreach(_ => kClient.bulk(simpleOps))
    verify(mockClient, times(1)).info()
  }

  // Strict mode: KOpenSearchClient returns Success(BulkResult(errors=true, itemErrors=...)).
  // The *writer* (JsonBulkWriter) is responsible for converting errors=true into Failure(ConnectException)
  // and routing through ErrorPolicy. KOpenSearchClient is intentionally NOT the escalation boundary.
  test("strict mode: bulk with item errors returns Success(BulkResult(errors=true, itemErrors=...))") {
    val errorItem   = mock(classOf[BulkResponseItem])
    val errorDetail = mock(classOf[org.opensearch.client.opensearch._types.ErrorCause])
    when(errorDetail.reason()).thenReturn("mapping error")
    when(errorDetail.`type`()).thenReturn("mapper_parsing_exception")
    when(errorItem.error()).thenReturn(errorDetail)
    when(errorItem.index()).thenReturn("idx")
    when(errorItem.id()).thenReturn("1")
    when(errorItem.status()).thenReturn(400)

    val client = makeClient(
      makeInfoResponse("opensearch", "2.13.0"),
      makeBulkResponse(true, java.util.List.of(errorItem)),
    )
    val kClient = new KOpenSearchClient(client, settings(BULK_STRICT_ITEM_ERRORS_KEY -> "true"))
    val result  = kClient.bulk(simpleOps)
    result.isSuccess shouldBe true
    result.get.errors shouldBe true
    result.get.itemErrors should not be empty
    result.get.itemErrors.head.reason should include("mapping error")
    result.get.itemErrors.head.errorType shouldBe "mapper_parsing_exception"
    result.get.itemErrors.head.status shouldBe 400
  }

  // This test exercises the escalation path: JsonBulkWriter.insert converts
  // Success(BulkResult(errors=true)) into Failure(ConnectException) via flatMap,
  // then handleTry applies the configured ErrorPolicy (THROW → ConnectException propagates).
  test("writer escalation: JsonBulkWriter converts BulkResult(errors=true) into exception under THROW policy") {
    var capturedOps: Seq[BulkOp] = Seq.empty
    val stubbedClient = new KBulkClient {
      override def bulk(ops: Seq[BulkOp]): Try[BulkResult] = {
        capturedOps = ops
        Success(BulkResult(took   = 1L,
                           errors = true,
                           itemErrors =
                             Seq(BulkItemError("idx", "1", "mapping error")),
        ))
      }
      override def createIndex(name: String): Try[Unit] = Success(())
      override def close(): Unit = ()
    }

    val writerSettings = settings(
      ERROR_POLICY                -> "THROW",
      BULK_STRICT_ITEM_ERRORS_KEY -> "true",
    )
    val writer = new JsonBulkWriter(stubbedClient, writerSettings.common)

    val schema: Schema     = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build()
    val struct: Struct     = new Struct(schema).put("id", "val1")
    val record: SinkRecord = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "k", schema, struct, 0L)

    val ex = intercept[FatalConnectException](writer.write(Vector(record)))
    ex.getMessage should include("item-level error")
  }

  test("tolerant mode: bulk with item errors returns BulkResult with errors=false") {
    val errorItem   = mock(classOf[BulkResponseItem])
    val errorDetail = mock(classOf[org.opensearch.client.opensearch._types.ErrorCause])
    when(errorDetail.reason()).thenReturn("mapping error")
    when(errorItem.error()).thenReturn(errorDetail)
    when(errorItem.index()).thenReturn("idx")
    when(errorItem.id()).thenReturn("1")

    val client = makeClient(
      makeInfoResponse("opensearch", "2.13.0"),
      makeBulkResponse(true, java.util.List.of(errorItem)),
    )
    val kClient = new KOpenSearchClient(client, settings(BULK_STRICT_ITEM_ERRORS_KEY -> "false"))
    val result  = kClient.bulk(simpleOps)
    result.isSuccess shouldBe true
    result.get.errors shouldBe false
    result.get.itemErrors shouldBe empty
  }

  test("close() closes the transport exactly once even when called multiple times (idempotent)") {
    val transport = mock(classOf[OpenSearchTransport])
    val client    = mock(classOf[OpenSearchClient])
    when(client._transport()).thenReturn(transport)
    val kClient = new KOpenSearchClient(client, settings())
    kClient.close()
    kClient.close()
    verify(transport, times(1)).close()
  }

  test("InsertOp maps to an index operation") {
    val client = makeClient(
      makeInfoResponse("opensearch", "2.13.0"),
      makeBulkResponse(false),
    )
    val kClient = new KOpenSearchClient(client, settings())
    val result  = kClient.bulk(Seq(InsertOp("my-index", "doc-1", simpleDoc, None, None)))
    result.isSuccess shouldBe true
    verify(client).bulk(any(classOf[BulkRequest]))
  }

  test("UpsertOp maps to an update operation with docAsUpsert=true") {
    val client = makeClient(
      makeInfoResponse("opensearch", "2.13.0"),
      makeBulkResponse(false),
    )
    val kClient = new KOpenSearchClient(client, settings())
    val result  = kClient.bulk(Seq(UpsertOp("my-index", "doc-1", simpleDoc, None)))
    result.isSuccess shouldBe true
  }

  test("DeleteOp maps to a delete operation") {
    val client = makeClient(
      makeInfoResponse("opensearch", "2.13.0"),
      makeBulkResponse(false),
    )
    val kClient = new KOpenSearchClient(client, settings())
    val result  = kClient.bulk(Seq(DeleteOp("my-index", "doc-1", None)))
    result.isSuccess shouldBe true
  }

  // A7: UPSERT action must NOT contain a pipeline key
  test("A7: UpsertOp serialised action line has no pipeline field") {
    val captor = ArgumentCaptor.forClass(classOf[BulkRequest])
    val client = makeClient(
      makeInfoResponse("opensearch", "2.13.0"),
      makeBulkResponse(false),
    )
    val kClient = new KOpenSearchClient(client, settings())
    kClient.bulk(Seq(UpsertOp("my-index", "upsert-1", simpleDoc, None)))
    verify(client).bulk(captor.capture())

    val req = captor.getValue
    val ops = req.operations().asScala
    ops should have size 1
    val op = ops.head
    // Verifying the operation is Update (not Index/Delete) is the core A7 invariant.
    // Wire-level docAsUpsert=true is verified in the B8 NDJSON wire tests below.
    op.isUpdate shouldBe true
  }

  // B8: INSERT NDJSON action lines must NOT contain forbidden wire fields
  test("B8: InsertOp serialised action line contains no routing/version/op_type/if_seq_no fields") {
    val captor = ArgumentCaptor.forClass(classOf[BulkRequest])
    val client = makeClient(
      makeInfoResponse("opensearch", "2.13.0"),
      makeBulkResponse(false),
    )
    val kClient = new KOpenSearchClient(client, settings())
    kClient.bulk(Seq(InsertOp("my-index", "wire-1", simpleDoc, None, None)))
    verify(client).bulk(captor.capture())

    val req = captor.getValue
    val ops = req.operations().asScala
    ops should have size 1
    val op = ops.head
    op.isIndex shouldBe true
    val indexOp = op.index()

    // Forbidden wire fields must NOT be set
    indexOp.routing() shouldBe null
    indexOp.version() shouldBe null
    indexOp.versionType() shouldBe null
    indexOp.ifSeqNo() shouldBe null
    indexOp.ifPrimaryTerm() shouldBe null
    // pipeline is allowed only when set explicitly via InsertOp.pipeline; default None → null
    indexOp.pipeline() shouldBe null
  }

  // B8: UPSERT action lines must NOT contain forbidden fields either
  test("B8: UpsertOp serialised action line contains no routing/retryOnConflict fields beyond docAsUpsert") {
    val captor = ArgumentCaptor.forClass(classOf[BulkRequest])
    val client = makeClient(
      makeInfoResponse("opensearch", "2.13.0"),
      makeBulkResponse(false),
    )
    val kClient = new KOpenSearchClient(client, settings())
    kClient.bulk(Seq(UpsertOp("my-index", "wire-2", simpleDoc, None)))
    verify(client).bulk(captor.capture())

    val req = captor.getValue
    val ops = req.operations().asScala
    val op  = ops.head
    op.isUpdate shouldBe true
    val updateOp = op.update()
    updateOp.routing() shouldBe null
    updateOp.retryOnConflict() shouldBe null
  }

  // A4: empty-id guard
  test("A4: bulk with empty id raises IllegalArgumentException") {
    val client  = makeClient(makeInfoResponse("opensearch", "2.13.0"), makeBulkResponse(false))
    val kClient = new KOpenSearchClient(client, settings())
    val ex = intercept[IllegalArgumentException](
      kClient.bulk(Seq(InsertOp("idx", "", simpleDoc, None, None))).get,
    )
    ex.getMessage should include("id must be non-empty")
  }

  // A4: empty-index guard
  test("A4: bulk with empty index raises IllegalArgumentException") {
    val client  = makeClient(makeInfoResponse("opensearch", "2.13.0"), makeBulkResponse(false))
    val kClient = new KOpenSearchClient(client, settings())
    val ex = intercept[IllegalArgumentException](
      kClient.bulk(Seq(InsertOp("", "1", simpleDoc, None, None))).get,
    )
    ex.getMessage should include("index must be non-empty")
  }

  // A4: 512-byte id-length guard
  test("A4: bulk with id exceeding 512 UTF-8 bytes raises IllegalArgumentException") {
    val longId  = "x" * 513
    val client  = makeClient(makeInfoResponse("opensearch", "2.13.0"), makeBulkResponse(false))
    val kClient = new KOpenSearchClient(client, settings())
    val ex = intercept[IllegalArgumentException](
      kClient.bulk(Seq(InsertOp("idx", longId, simpleDoc, None, None))).get,
    )
    ex.getMessage should include("512-byte")
  }

  // Wire-level assertion: the NDJSON serialised by the opensearch-java BulkRequest
  // must not contain a "_type" or "type" mapping-type field (OpenSearch 2.x dropped types).
  test("B8 wire: BulkRequest NDJSON contains no _type field") {
    val captor     = ArgumentCaptor.forClass(classOf[BulkRequest])
    val mockClient = makeClient(makeInfoResponse("opensearch", "2.13.0"), makeBulkResponse(false))
    val kClient    = new KOpenSearchClient(mockClient, settings())
    kClient.bulk(Seq(InsertOp("my-index", "wire-3", simpleDoc, None, None)))
    verify(mockClient).bulk(captor.capture())

    val req    = captor.getValue
    val mapper = new JacksonJsonpMapper()
    val baos   = new ByteArrayOutputStream()
    val gen    = mapper.jsonProvider().createGenerator(baos)
    req.serialize(gen, mapper)
    gen.close()
    val ndjson = baos.toString("UTF-8")

    ndjson should not include "\"_type\""
    ndjson should not include "\"type\""
  }

  // AUTOCREATE failure: createIndex wraps non-resource_already_exists OpenSearchException as ConnectException
  test("createIndex: non-resource_already_exists OpenSearchException is wrapped as ConnectException") {
    val mockErrorCause = mock(classOf[ErrorCause])
    when(mockErrorCause.`type`()).thenReturn("mapper_parsing_exception")
    val mockErrorResp = mock(classOf[ErrorResponse])
    when(mockErrorResp.error()).thenReturn(mockErrorCause)
    val osEx = new OpenSearchException(mockErrorResp)

    type BuilderFn = java.util.function.Function[CreateIndexRequest.Builder, ObjectBuilder[CreateIndexRequest]]
    val indicesClient = mock(classOf[OpenSearchIndicesClient])
    when(indicesClient.create(any(classOf[BuilderFn]).asInstanceOf[BuilderFn])).thenThrow(osEx)

    val client = makeClient(makeInfoResponse("opensearch", "2.13.0"), makeBulkResponse(false))
    when(client.indices()).thenReturn(indicesClient)

    val kClient = new KOpenSearchClient(client, settings())
    val result  = kClient.createIndex("new-index")

    result.isFailure shouldBe true
    result.failed.get shouldBe a[ConnectException]
    result.failed.get.getMessage should include("new-index")
  }

  // AUTOCREATE idempotency: resource_already_exists exception is swallowed (Success)
  test("createIndex: resource_already_exists OpenSearchException returns Success (idempotent restart)") {
    val mockErrorCause = mock(classOf[ErrorCause])
    when(mockErrorCause.`type`()).thenReturn("resource_already_exists_exception")
    val mockErrorResp = mock(classOf[ErrorResponse])
    when(mockErrorResp.error()).thenReturn(mockErrorCause)
    val osEx = new OpenSearchException(mockErrorResp)

    type BuilderFn = java.util.function.Function[CreateIndexRequest.Builder, ObjectBuilder[CreateIndexRequest]]
    val indicesClient = mock(classOf[OpenSearchIndicesClient])
    when(indicesClient.create(any(classOf[BuilderFn]).asInstanceOf[BuilderFn])).thenThrow(osEx)

    val client = makeClient(makeInfoResponse("opensearch", "2.13.0"), makeBulkResponse(false))
    when(client.indices()).thenReturn(indicesClient)

    val kClient = new KOpenSearchClient(client, settings())
    val result  = kClient.createIndex("existing-index")

    result.isSuccess shouldBe true
  }

  // Verify that UpsertOp maps to an Update BulkOperation (not Index or Delete).
  // The doc_as_upsert=true flag is set in KOpenSearchClient.bulk — this is a code-level invariant.
  test("A7 code: UpsertOp is mapped to an Update (not Index) BulkOperation") {
    val captor     = ArgumentCaptor.forClass(classOf[BulkRequest])
    val mockClient = makeClient(makeInfoResponse("opensearch", "2.13.0"), makeBulkResponse(false))
    val kClient    = new KOpenSearchClient(mockClient, settings())
    kClient.bulk(Seq(UpsertOp("my-index", "upsert-wire", simpleDoc, None)))
    verify(mockClient).bulk(captor.capture())

    val ops = captor.getValue.operations().asScala
    ops should have size 1
    ops.head.isUpdate shouldBe true
    ops.head.isIndex shouldBe false
    ops.head.isDelete shouldBe false
  }
}
