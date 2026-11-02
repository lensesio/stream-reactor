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

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.lenses.streamreactor.common.errors.RetriableIntegrityException
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.errors.RetriableException
import org.apache.kafka.connect.sink.SinkRecord
import org.opensearch.client.opensearch.OpenSearchClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Integration tests for error policy end-to-end behaviour.
 *
 * Uses WireMock to inject transport-level failures:
 *  - RETRY policy: WireMock returns HTTP 503 → `RetriableException` propagates from the writer.
 *  - NOOP policy: WireMock returns HTTP 503 → the transport failure is reclassified as a
 *    `RetriableIntegrityException` (a `FatalConnectException`) and rethrown fail-fast rather than
 *    silently swallowed, so offsets never advance past a batch that was never written.
 *  - THROW policy: WireMock returns HTTP 503 → `ConnectException` propagates.
 */
class OpenSearchErrorPolicyIT extends AnyFunSuite with Matchers with BeforeAndAfterAll with ITResourceRegistry {

  private var wireMock: WireMockServer = _

  override def beforeAll(): Unit = {
    wireMock = new WireMockServer(wireMockConfig().dynamicPort())
    wireMock.start()
  }

  override def afterAll(): Unit =
    wireMock.stop()

  private val schema: Schema = SchemaBuilder.struct()
    .field("id", Schema.STRING_SCHEMA)
    .build()

  private def sinkRecord(topic: String, id: String): SinkRecord = {
    val struct = new Struct(schema).put("id", id)
    new SinkRecord(topic, 0, Schema.STRING_SCHEMA, id, schema, struct, 0L)
  }

  private def makeWriter(host: String, port: Int, kcql: String, errorPolicy: String): JsonBulkWriter = {
    val props = Map(
      HOSTS          -> host,
      ES_PORT        -> port.toString,
      KCQL           -> kcql,
      ERROR_POLICY   -> errorPolicy,
      NBR_OF_RETRIES -> "2",
    )
    val config    = OpenSearchConfig(props)
    val settings  = OpenSearchSettings(config)
    val transport = track(OpenSearchTransportFactory.create(settings))
    val osClient  = new OpenSearchClient(transport)
    val kClient   = new KOpenSearchClient(osClient, settings)
    new JsonBulkWriter(kClient, settings.common)
  }

  private val infoBody =
    """{
      |  "name": "test",
      |  "cluster_name": "test",
      |  "cluster_uuid": "test-cluster-uuid",
      |  "version": {
      |    "distribution": "opensearch",
      |    "number": "2.13.0",
      |    "build_type": "tar",
      |    "build_hash": "0000000000000000000000000000000000000000",
      |    "build_date": "2024-01-01T00:00:00.000Z",
      |    "build_snapshot": false,
      |    "lucene_version": "9.10.0",
      |    "minimum_wire_compatibility_version": "7.10.0",
      |    "minimum_index_compatibility_version": "7.0.0",
      |    "build_flavor": "default"
      |  },
      |  "tagline": "The OpenSearch Project"
      |}""".stripMargin

  test("THROW policy: transport error from WireMock 503 surfaces as ConnectException") {
    wireMock.resetAll()
    wireMock.stubFor(
      get(urlEqualTo("/")).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(infoBody),
      ),
    )
    wireMock.stubFor(
      post(urlEqualTo("/_bulk")).willReturn(
        aResponse().withStatus(503).withHeader("Content-Type", "application/json")
          .withBody("""{"error":"service unavailable"}"""),
      ),
    )

    val writer = makeWriter("localhost", wireMock.port(), "INSERT INTO idx SELECT * FROM topic PK id", "THROW")
    intercept[ConnectException](writer.write(Vector(sinkRecord("topic", "id-1"))))
  }

  test("RETRY policy: transport error from WireMock 503 surfaces as RetriableException") {
    wireMock.resetAll()
    wireMock.stubFor(
      get(urlEqualTo("/")).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(infoBody),
      ),
    )
    wireMock.stubFor(
      post(urlEqualTo("/_bulk")).willReturn(
        aResponse().withStatus(503).withHeader("Content-Type", "application/json")
          .withBody("""{"error":"service unavailable"}"""),
      ),
    )

    val writer = makeWriter("localhost", wireMock.port(), "INSERT INTO idx SELECT * FROM topic PK id", "RETRY")
    intercept[RetriableException](writer.write(Vector(sinkRecord("topic", "id-2"))))
  }

  test("NOOP policy: transport error from WireMock 503 is reclassified and rethrown fail-fast") {
    wireMock.resetAll()
    wireMock.stubFor(
      get(urlEqualTo("/")).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(infoBody),
      ),
    )
    wireMock.stubFor(
      post(urlEqualTo("/_bulk")).willReturn(
        aResponse().withStatus(503).withHeader("Content-Type", "application/json")
          .withBody("""{"error":"service unavailable"}"""),
      ),
    )

    // A whole-call transport failure is reclassified as RetriableIntegrityException, which is a
    // FatalConnectException. NOOP must NOT swallow it: swallowing would advance offsets past a
    // batch that was never written (silent data loss).
    val writer = makeWriter("localhost", wireMock.port(), "INSERT INTO idx SELECT * FROM topic PK id", "NOOP")
    intercept[RetriableIntegrityException](writer.write(Vector(sinkRecord("topic", "id-3"))))
  }
}
