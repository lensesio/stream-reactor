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

import io.lenses.streamreactor.connect.elastic.common.config.ItemErrorMode
import io.lenses.streamreactor.connect.elastic7.config.ElasticConfig
import io.lenses.streamreactor.connect.elastic7.config.ElasticConfigConstants
import io.lenses.streamreactor.connect.elastic7.config.ElasticSettings
import org.apache.kafka.common.config.ConfigException

class ElasticConfigTest extends TestBase {
  "A ElasticConfig should return the client mode and hostnames" in {
    val config = new ElasticConfig(getElasticSinkConfigProps())
    config.getString(ElasticConfigConstants.HOSTS) shouldBe ELASTIC_SEARCH_HOSTNAMES
    config.getString(ElasticConfigConstants.ES_CLUSTER_NAME) shouldBe ElasticConfigConstants.ES_CLUSTER_NAME_DEFAULT
    config.getString(ElasticConfigConstants.KCQL) shouldBe QUERY
  }

  "A ElasticConfig should default bulk.strict.item.errors to true and write.timeout to 300000" in {
    val config = new ElasticConfig(getElasticSinkConfigProps())
    config.getBoolean(ElasticConfigConstants.BULK_STRICT_ITEM_ERRORS_KEY) shouldBe true
    config.getInt(ElasticConfigConstants.WRITE_TIMEOUT_CONFIG) shouldBe 300000
  }

  "A ElasticConfig should honour bulk.strict.item.errors=false" in {
    val config = new ElasticConfig(
      getElasticSinkConfigProps() + (ElasticConfigConstants.BULK_STRICT_ITEM_ERRORS_KEY -> "false"),
    )
    config.getBoolean(ElasticConfigConstants.BULK_STRICT_ITEM_ERRORS_KEY) shouldBe false
  }

  "ElasticSettings should resolve bulk.item.errors into ItemErrorMode.Fail by default" in {
    val config = new ElasticConfig(getElasticSinkConfigProps())
    ElasticSettings(config).itemErrorMode shouldBe ItemErrorMode.Fail
  }

  "ElasticSettings should resolve bulk.item.errors=drop into ItemErrorMode.Drop" in {
    val config = new ElasticConfig(
      getElasticSinkConfigProps() + (ElasticConfigConstants.BULK_ITEM_ERRORS_KEY -> "drop"),
    )
    ElasticSettings(config).itemErrorMode shouldBe ItemErrorMode.Drop
  }

  "ElasticSettings should resolve bulk.item.errors=dlq into ItemErrorMode.Dlq" in {
    val config = new ElasticConfig(
      getElasticSinkConfigProps() + (ElasticConfigConstants.BULK_ITEM_ERRORS_KEY -> "dlq"),
    )
    ElasticSettings(config).itemErrorMode shouldBe ItemErrorMode.Dlq
  }

  // The ConfigDef validator (CaseInsensitiveValidString.in("fail", "drop", "dlq")) now rejects an
  // invalid value at ElasticConfig construction — the earliest possible point, e.g. Connect's
  // /connectors/validate endpoint — rather than only surfacing later at ElasticSettings
  // resolution (which ItemErrorMode.fromString still guards as a defence-in-depth backstop
  // for any other caller that bypasses the ConfigDef).
  "A ElasticConfig should reject an invalid bulk.item.errors value at construction" in {
    val ex = intercept[ConfigException] {
      new ElasticConfig(
        getElasticSinkConfigProps() + (ElasticConfigConstants.BULK_ITEM_ERRORS_KEY -> "maybe"),
      )
    }
    ex.getMessage should include(ElasticConfigConstants.BULK_ITEM_ERRORS_KEY)
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

  "A ElasticConfig should accept write.timeout of 1000 milliseconds" in {
    val config = new ElasticConfig(
      getElasticSinkConfigProps() + (ElasticConfigConstants.WRITE_TIMEOUT_CONFIG -> "1000"),
    )
    config.getInt(ElasticConfigConstants.WRITE_TIMEOUT_CONFIG) shouldBe 1000
  }

  "A ElasticConfig should return the http basic auth username and password when set" in {
    val config = new ElasticConfig(getElasticSinkConfigPropsHTTPClient(auth = true))
    config.getString(ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_USERNAME) shouldBe BASIC_AUTH_USERNAME
    config.getString(ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_PASSWORD) shouldBe BASIC_AUTH_PASSWORD
  }
}
