/*
 * Copyright 2017-2024 Lenses.io Ltd
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

import io.lenses.streamreactor.connect.elastic6.config.ElasticConfig
import io.lenses.streamreactor.connect.elastic6.config.ElasticSettings

class ElasticWriterCredentialsTest extends TestBase {

  "A writer should be using HTTP is set with HTTP Basic Auth Credentials" in {
    val config   = new ElasticConfig(getElasticSinkConfigPropsHTTPClient(auth = true))
    val settings = ElasticSettings(config)
    settings.httpBasicAuthUsername shouldBe BASIC_AUTH_USERNAME
    settings.httpBasicAuthPassword shouldBe BASIC_AUTH_PASSWORD
  }
}
