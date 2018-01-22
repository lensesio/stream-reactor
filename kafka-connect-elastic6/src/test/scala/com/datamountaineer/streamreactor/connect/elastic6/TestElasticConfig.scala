/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.elastic6

import com.datamountaineer.streamreactor.connect.elastic6.config.{ElasticConfig, ElasticConfigConstants}

class TestElasticConfig extends TestElasticBase {
  "A ElasticConfig should return the client mode and hostnames" in {
    val config = new ElasticConfig(getElasticSinkConfigProps)
    config.getString(ElasticConfigConstants.URL) shouldBe ELASTIC_SEARCH_HOSTNAMES
    config.getString(ElasticConfigConstants.ES_CLUSTER_NAME) shouldBe ElasticConfigConstants.ES_CLUSTER_NAME_DEFAULT
    config.getString(ElasticConfigConstants.KCQL) shouldBe QUERY
  }
}