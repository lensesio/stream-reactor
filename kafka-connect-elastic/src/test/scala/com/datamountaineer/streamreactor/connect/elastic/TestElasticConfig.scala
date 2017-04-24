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

package com.datamountaineer.streamreactor.connect.elastic

import com.datamountaineer.streamreactor.connect.elastic.config.{ElasticSinkConfig, ElasticSinkConfigConstants}

class TestElasticConfig extends TestElasticBase {
  "A ElasticConfig should return the client mode and hostnames" in {
    val config = new ElasticSinkConfig(getElasticSinkConfigProps)
    config.getString(ElasticSinkConfigConstants.URL) shouldBe ELASTIC_SEARCH_HOSTNAMES
    config.getString(ElasticSinkConfigConstants.ES_CLUSTER_NAME) shouldBe ElasticSinkConfigConstants.ES_CLUSTER_NAME_DEFAULT
    config.getString(ElasticSinkConfigConstants.EXPORT_ROUTE_QUERY) shouldBe QUERY
  }
}