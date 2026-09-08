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

import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
import org.apache.kafka.common.config.ConfigException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Explicit parity-contract tests documenting intentional OpenSearch vs Elasticsearch 7 differences.
 *
 * Each test is labelled with its parity decision so that future refactors cannot accidentally
 * "fix" a known intentional divergence without breaking an explicit test.
 *
 * === Intentional differences from ES7 ===
 *
 * 1. [[BULK_STRICT_ITEM_ERRORS_KEY]] defaults to `true` (same as ES6/ES7). Setting it `false`
 *    restores legacy tolerant mode that swallows per-item bulk failures.
 *
 * 2. [[ES_PREFIX]] (tableprefix) is rejected. The HC5 transport has no path-prefix API.
 *
 * 3. `connect.opensearch.cluster.name` is rejected at startup. `cluster.name` is an Elasticsearch
 *    concept; OpenSearch does not use it, and silently accepting it would confuse ES→OS migrants.
 *
 * 4. [[WITHDOCTYPE]] KCQL clause is silently ignored on OpenSearch (and ES7). It is NOT ignored
 *    on ES6, where `KElastic6BulkClient.supportsDocumentType == true` prevents the warning.
 *
 * 5. `write.timeout` is milliseconds on ES6, ES7 and OpenSearch (default 300000 = 5 minutes).
 *    ES6/ES7 additionally reject 1..120 as likely pre-migration seconds; OpenSearch does not.
 */
class OpenSearchParityContractTest extends AnyFunSuite with Matchers {

  private val baseProps = Map(
    HOSTS   -> "localhost",
    ES_PORT -> "9200",
    KCQL    -> "INSERT INTO idx SELECT * FROM topic",
  )

  private def settings(extra: (String, String)*): OpenSearchSettings =
    OpenSearchSettings(OpenSearchConfig(baseProps ++ extra.toMap))

  // --- 1. Bulk item-error strictness ---

  test("PARITY-1: bulk.strict.item.errors defaults to true") {
    settings().strictItemErrors shouldBe true
  }

  test("PARITY-1: bulk.strict.item.errors=false restores legacy tolerant mode") {
    settings(BULK_STRICT_ITEM_ERRORS_KEY -> "false").strictItemErrors shouldBe false
  }

  // --- 2. tableprefix rejection ---

  test("PARITY-2: tableprefix is not supported and is rejected with a descriptive ConfigException") {
    val ex = intercept[ConfigException](settings(ES_PREFIX -> "prefix/"))
    ex.getMessage should include("connect.opensearch.tableprefix is not supported")
    ex.getMessage should include("intentional difference")
  }

  // --- 3. cluster.name rejection ---

  test("PARITY-3: cluster.name key is rejected because it is an Elasticsearch-only concept") {
    val ex = intercept[ConfigException](
      settings(s"$CONNECTOR_PREFIX.cluster.name" -> "my-cluster"),
    )
    ex.getMessage should include("cluster.name")
    ex.getMessage should include("Elasticsearch concept")
  }

  // --- 4. WITHDOCTYPE: OpenSearch client does not support document types ---

  test("PARITY-4: KOpenSearchClient.supportsDocumentType is false (ES7 and OS dropped mapping types)") {
    import org.mockito.Mockito._
    import org.opensearch.client.opensearch.OpenSearchClient
    import org.opensearch.client.opensearch._types.OpenSearchVersionInfo
    import org.opensearch.client.opensearch.core.InfoResponse
    import org.opensearch.client.transport.OpenSearchTransport

    val versionInfo = mock(classOf[OpenSearchVersionInfo])
    when(versionInfo.distribution()).thenReturn("opensearch")
    when(versionInfo.number()).thenReturn("2.13.0")
    val infoResp = mock(classOf[InfoResponse])
    when(infoResp.version()).thenReturn(versionInfo)

    val transport = mock(classOf[OpenSearchTransport])
    val client    = mock(classOf[OpenSearchClient])
    when(client._transport()).thenReturn(transport)
    when(client.info()).thenReturn(infoResp)

    val kClient = new KOpenSearchClient(client, settings())
    kClient.supportsDocumentType shouldBe false
  }

  // --- 5. write.timeout unit differs across connectors ---

  test("PARITY-5: write.timeout default is 300000 milliseconds with a 1ms floor") {
    import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonConfigConstants
    ElasticCommonConfigConstants.WRITE_TIMEOUT_DEFAULT shouldBe 300000
    ElasticCommonConfigConstants.WRITE_TIMEOUT_MIN shouldBe 1
  }

  test("PARITY-5b: write.timeout 1..120 is allowed on OpenSearch (no ES6/ES7 seconds trap)") {
    settings(WRITE_TIMEOUT -> "60").common.writeTimeout shouldBe 60
    settings(WRITE_TIMEOUT -> "120").common.writeTimeout shouldBe 120
  }
}
