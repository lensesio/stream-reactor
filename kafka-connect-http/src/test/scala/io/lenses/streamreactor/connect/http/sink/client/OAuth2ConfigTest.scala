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
package io.lenses.streamreactor.connect.http.sink.client

import io.lenses.streamreactor.connect.http.sink.client.oauth2.OAuth2Config
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class OAuth2ConfigTest extends AnyFunSuite with Matchers with EitherValues {

  test("should fail if the token URL is not valid") {
    val invalidConfig = Map(
      OAuth2Config.OAuth2TokenUrlProp -> "not a url",
    )
    val result = OAuth2Authentication.from(createConfig(invalidConfig))

    result.left.value.getMessage should include(
      "OAuth2 authentication requires connect.http.authentication.oauth2.token.url to be set",
    )
  }

  test("picks the client id") {
    val config =
      Map(
        OAuth2Config.OAuth2TokenUrlProp -> "http://localhost:8080",
        OAuth2Config.OAuth2ClientIdProp -> "client-id",
      )
    val result = OAuth2Authentication.from(createConfig(config))
    result.value.clientId should be("client-id")
  }

  test("picks the client secret") {
    val config =
      Map(
        OAuth2Config.OAuth2TokenUrlProp     -> "http://localhost:8080",
        OAuth2Config.OAuth2ClientSecretProp -> "client-secret",
      )
    val result = OAuth2Authentication.from(createConfig(config))
    result.value.clientSecret should be("client-secret")
  }

  test("overrwrites the access_token field") {
    val config =
      Map(
        OAuth2Config.OAuth2TokenUrlProp         -> "http://localhost:8080",
        OAuth2Config.OAuth2AccessTokenFieldProp -> "access_token_field",
      )
    val result = OAuth2Authentication.from(createConfig(config))
    result.value.tokenProperty should be("access_token_field")
  }

  test("picks the client scope") {
    val config =
      Map(
        OAuth2Config.OAuth2TokenUrlProp    -> "http://localhost:8080",
        OAuth2Config.OAuth2ClientScopeProp -> "client-scope",
      )
    val result = OAuth2Authentication.from(createConfig(config))
    result.value.clientScope should be(Some("client-scope"))
  }
  test("handle the headers which is a Kafka Connect list") {
    val config: Map[String, Any] = Map(
      OAuth2Config.OAuth2TokenUrlProp               -> "http://localhost:8080",
      OAuth2Config.OAuth2ClientHeadersProp          -> List("header1:value1", "header2:value2").asJava,
      OAuth2Config.OAuth2ClientHeadersSeparatorProp -> ":",
    )
    val result = OAuth2Authentication.from(createConfig(config))
    result.value.clientHeaders should be(List("header1" -> "value1", "header2" -> "value2"))
  }
  private def createConfig(configMap: Map[String, Any]): AbstractConfig = {
    val configDef = OAuth2Config.append(new ConfigDef())
    new AbstractConfig(configDef, configMap.asJava)
  }
}
