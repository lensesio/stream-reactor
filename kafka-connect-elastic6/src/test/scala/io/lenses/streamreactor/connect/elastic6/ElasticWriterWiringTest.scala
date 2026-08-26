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
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import io.lenses.kcql.Kcql
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

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
class ElasticWriterWiringTest extends AnyWordSpec with Matchers with MockitoSugar with ArgumentMatchersSugar {

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
  }
}
