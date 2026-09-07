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

import io.lenses.streamreactor.connect.elastic.common.config.ItemErrorDlqMode
import io.lenses.streamreactor.connect.elastic6.config.ElasticConfig
import io.lenses.streamreactor.connect.elastic6.config.ElasticConfigConstants
import io.lenses.streamreactor.connect.elastic6.config.ElasticSettings
import org.apache.kafka.common.config.ConfigException

class ElasticConfigTest extends TestBase {
  "A ElasticConfig should return the client mode and hostnames" in {
    val config = new ElasticConfig(getElasticSinkConfigProps())
    config.getString(ElasticConfigConstants.HOSTS) shouldBe ELASTIC_SEARCH_HOSTNAMES
    config.getString(ElasticConfigConstants.ES_CLUSTER_NAME) shouldBe ElasticConfigConstants.ES_CLUSTER_NAME_DEFAULT
    config.getString(ElasticConfigConstants.KCQL) shouldBe QUERY
  }

  "A ElasticConfig should return the http basic auth username and password when set" in {
    val config = new ElasticConfig(getElasticSinkConfigPropsHTTPClient(auth = true))
    config.getString(ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_USERNAME) shouldBe BASIC_AUTH_USERNAME
    config.getString(ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_PASSWORD) shouldBe BASIC_AUTH_PASSWORD
  }

  "A ElasticConfig should default bulk.strict.item.errors to true and write.timeout to 300000" in {
    val config = new ElasticConfig(getElasticSinkConfigProps())
    config.getBoolean(ElasticConfigConstants.BULK_STRICT_ITEM_ERRORS_KEY) shouldBe true
    config.getInt(ElasticConfigConstants.WRITE_TIMEOUT_CONFIG) shouldBe 300000
  }

  "A ElasticConfig should reject write.timeout values that look like pre-migration seconds" in {
    val ex = intercept[ConfigException] {
      new ElasticConfig(getElasticSinkConfigProps() + (ElasticConfigConstants.WRITE_TIMEOUT_CONFIG -> "60"))
    }
    ex.getMessage should include("write.timeout")
    ex.getMessage should include("seconds")
  }

  "A ElasticConfig should accept write.timeout of 750 milliseconds" in {
    val config = new ElasticConfig(
      getElasticSinkConfigProps() + (ElasticConfigConstants.WRITE_TIMEOUT_CONFIG -> "750"),
    )
    config.getInt(ElasticConfigConstants.WRITE_TIMEOUT_CONFIG) shouldBe 750
  }

  // ---- ES6 is unaffected by the per-record DLQ feature (lensesio-dev/stream-reactor#414) ----
  //
  // ES6's ConfigDef never registers bulk.item.errors.dlq at all (unlike ES7/OpenSearch), and
  // KElastic6BulkClient never populates BulkItemError.position — verify BOTH halves of that
  // claim: the key is genuinely absent (a user setting it is silently ignored, not rejected,
  // per Kafka Connect's own unknown-property handling), AND itemErrorDlqMode stays pinned to
  // Fail regardless of what a user sets that (ignored) key to.

  "A ElasticConfig should silently ignore an unknown bulk.item.errors.dlq property rather than reject or crash" in {
    val unknownKey = s"${ElasticConfigConstants.CONNECTOR_PREFIX}.bulk.item.errors.dlq"
    noException should be thrownBy new ElasticConfig(getElasticSinkConfigProps() + (unknownKey -> "skip"))
  }

  "ElasticSettings should stay pinned to ItemErrorDlqMode.Fail even when the (ignored) bulk.item.errors.dlq key is set to skip" in {
    val unknownKey = s"${ElasticConfigConstants.CONNECTOR_PREFIX}.bulk.item.errors.dlq"
    val config     = new ElasticConfig(getElasticSinkConfigProps() + (unknownKey -> "skip"))
    ElasticSettings(config).itemErrorDlqMode shouldBe ItemErrorDlqMode.Fail
  }

  "ElasticSettings should default to ItemErrorDlqMode.Fail with no bulk.item.errors.dlq key set at all" in {
    val config = new ElasticConfig(getElasticSinkConfigProps())
    ElasticSettings(config).itemErrorDlqMode shouldBe ItemErrorDlqMode.Fail
  }
}
