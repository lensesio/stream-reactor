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

import com.sksamuel.elastic4s.bulk.BulkRequest
import com.sksamuel.elastic4s.http.Response
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import io.lenses.streamreactor.common.errors.NoopErrorPolicy
import io.lenses.streamreactor.common.errors.RetriableIntegrityException
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings
import io.lenses.streamreactor.connect.elastic.common.config.ItemErrorMode
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import io.lenses.streamreactor.connect.elastic6.config.ElasticConfig
import io.lenses.streamreactor.connect.elastic6.config.ElasticConfigConstants
import io.lenses.streamreactor.connect.elastic6.config.ElasticSettings
import io.lenses.kcql.Kcql
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar

import java.util.Collections.emptyList
import java.util.Collections.emptyMap
import scala.concurrent.Future

/**
 * Smoke-wiring test: verifies that JsonBulkWriter → KElastic6BulkClient → KElasticClient.execute
 * is correctly wired.  Fine-grained writer behaviour is covered by
 * JsonBulkWriterTest in elastic-common.
 *
 * ES6-specific: also verifies that supportsDocumentType=true (WITHDOCTYPE is honoured).
 */
class ElasticWriterWiringTest extends TestBase with MockitoSugar with ArgumentMatchersSugar {

  "KElastic6BulkClient wiring" should {
    "call KElasticClient.execute for each write batch" in {
      val elasticClient = mock[KElasticClient]
      val mockBulkResp  = mock[Response[BulkResponse]]
      val bulkResult    = mock[BulkResponse]
      when(elasticClient.execute(any[BulkRequest])).thenReturn(Future.successful(mockBulkResp))
      when(mockBulkResp.isError).thenReturn(false)
      when(mockBulkResp.result).thenReturn(bulkResult)
      when(bulkResult.took).thenReturn(1L)
      when(bulkResult.items).thenReturn(Seq.empty)

      val bulkClient = new KElastic6BulkClient(elasticClient, writeTimeoutMillis = 5000)

      val sourceTopic = "source"
      val targetIndex = "target"
      val kcql        = mock[Kcql]
      when(kcql.getSource).thenReturn(sourceTopic)
      when(kcql.getTargetType).thenReturn(
        cyclops.control.Either.right[IllegalArgumentException, io.lenses.kcql.targettype.TargetType](
          new io.lenses.kcql.targettype.StaticTargetType(targetIndex),
        ),
      )
      when(kcql.getProperties).thenReturn(emptyMap)
      when(kcql.getWriteMode).thenReturn(io.lenses.kcql.WriteModeEnum.INSERT)
      when(kcql.getFields).thenReturn(emptyList())
      when(kcql.getIgnoredFields).thenReturn(emptyList())

      val record = mock[SinkRecord]
      when(record.topic()).thenReturn(sourceTopic)
      when(record.key()).thenReturn("k")
      when(record.value()).thenReturn("""{"a":1}""")
      when(record.valueSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA)

      val writer = new JsonBulkWriter(bulkClient, ElasticCommonSettings(Seq(kcql), new NoopErrorPolicy))
      writer.write(Vector(record))

      verify(elasticClient, times(1)).execute(any[BulkRequest])
    }

    "supportsDocumentType is true for ES6 (WITHDOCTYPE is honoured — no warning emitted)" in {
      val elasticClient = mock[KElasticClient]
      val bulkClient    = new KElastic6BulkClient(elasticClient, writeTimeoutMillis = 5000)
      bulkClient.supportsDocumentType shouldBe true
    }

    "a transport-level failure under NOOP fails the task instead of being silently swallowed" in {
      val elasticClient = mock[KElasticClient]
      when(elasticClient.execute(any[BulkRequest])).thenReturn(Future.failed(new RuntimeException("connection reset")))

      val bulkClient = new KElastic6BulkClient(elasticClient, writeTimeoutMillis = 5000)

      val sourceTopic = "source"
      val targetIndex = "target"
      val kcql        = mock[Kcql]
      when(kcql.getSource).thenReturn(sourceTopic)
      when(kcql.getTargetType).thenReturn(
        cyclops.control.Either.right[IllegalArgumentException, io.lenses.kcql.targettype.TargetType](
          new io.lenses.kcql.targettype.StaticTargetType(targetIndex),
        ),
      )
      when(kcql.getProperties).thenReturn(emptyMap)
      when(kcql.getWriteMode).thenReturn(io.lenses.kcql.WriteModeEnum.INSERT)
      when(kcql.getFields).thenReturn(emptyList())
      when(kcql.getIgnoredFields).thenReturn(emptyList())

      val record = mock[SinkRecord]
      when(record.topic()).thenReturn(sourceTopic)
      when(record.key()).thenReturn("k")
      when(record.value()).thenReturn("""{"a":1}""")
      when(record.valueSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA)

      val writer = new JsonBulkWriter(bulkClient, ElasticCommonSettings(Seq(kcql), new NoopErrorPolicy))

      val thrown = intercept[RetriableIntegrityException](writer.write(Vector(record)))
      thrown.getMessage should include("connection reset")
    }

    // F10: ElasticConfigTest already locks that ElasticSettings.itemErrorMode stays pinned to
    // Fail even when the (ignored) bulk.item.errors key is set to drop — ES6's ConfigDef never
    // registers it, and KElastic6BulkClient never populates BulkItemError.position. This proves
    // the end-to-end CONSEQUENCE through the real config-parsing path: a writer-stage failure
    // (a missing header used for the PK) still fails the task on ES6, not just theoretically —
    // JsonBulkWriter itself has no ES6-awareness and would happily divert this under
    // bulk.item.errors=drop on ES7/OpenSearch (see JsonBulkWriterTest); it is entirely
    // ElasticSettings never actually setting itemErrorMode to anything but Fail that keeps ES6
    // safe from silently discarding a misconfigured topic one record at a time.
    "a writer-stage failure still fails the task even with bulk.item.errors=drop set in raw config (ES6 never honours it)" in {
      val unknownKey       = s"${ElasticConfigConstants.CONNECTOR_PREFIX}.bulk.item.errors"
      val kcqlWithHeaderPk = s"INSERT INTO $INDEX SELECT * FROM $TOPIC PK _header.tenant_id"
      val config           = new ElasticConfig(getBaseElasticSinkConfigProps(kcqlWithHeaderPk) + (unknownKey -> "drop"))
      val settings         = ElasticSettings(config)
      settings.itemErrorMode shouldBe ItemErrorMode.Fail

      val elasticClient = mock[KElasticClient]
      val bulkClient    = new KElastic6BulkClient(elasticClient, writeTimeoutMillis = 5000)
      val writer        = new JsonBulkWriter(bulkClient, settings)

      // No "tenant_id" header on this record — a per-record writer-stage failure that WOULD be
      // divertible if drop actually took effect.
      val record = new SinkRecord(TOPIC, 0, Schema.STRING_SCHEMA, "k", Schema.STRING_SCHEMA, """{"a":1}""", 0L)

      intercept[IllegalArgumentException](writer.write(Vector(record)))
      verify(elasticClient, never).execute(any[BulkRequest])
    }
  }
}
