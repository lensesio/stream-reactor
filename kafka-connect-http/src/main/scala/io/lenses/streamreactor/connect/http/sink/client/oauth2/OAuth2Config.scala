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
package io.lenses.streamreactor.connect.http.sink.client.oauth2

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type
import scala.jdk.CollectionConverters._

object OAuth2Config {
  val OAuth2TokenUrlProp: String = "connect.http.authentication.oauth2.token.url"
  val OAuth2TokenUrlDoc: String =
    """
      |The URL to request the OAuth2 token from.
      |""".stripMargin
  val OAuth2TokenUrlDefault = ""

  val OAuth2ClientIdProp: String = "connect.http.authentication.oauth2.client.id"
  val OAuth2ClientIdDoc: String =
    """
      |The client ID for OAuth2 authentication.
      |""".stripMargin
  val OAuth2ClientIdDefault = ""

  val OAuth2ClientSecretProp: String = "connect.http.authentication.oauth2.client.secret"
  val OAuth2ClientSecretDoc: String =
    """
      |The client secret for OAuth2 authentication.
      |""".stripMargin
  val OAuth2ClientSecretDefault = ""

  val OAuth2AccessTokenFieldProp: String = "connect.http.authentication.oauth2.token.property"
  val OAuth2AccessTokenFieldDoc: String =
    """
      |The field name for the access token in the OAuth2 response. Usually it is `access_token`.
      |""".stripMargin
  val OAuth2AccessTokenDefault = "access_token"

  val OAuth2ClientScopeProp: String = "connect.http.authentication.oauth2.client.scope"
  val OAuth2ClientScopeDoc: String =
    """
      |The scope for the OAuth2 client.
      |""".stripMargin
  val OAuth2ClientScopeDefault = "any"

  val OAuth2ClientHeadersProp: String = "connect.http.authentication.oauth2.client.headers"
  val OAuth2ClientHeadersDoc: String =
    """
      |A list of headers to include in the OAuth2 request.
      |""".stripMargin
  val OAuth2ClientHeadersSeparatorProp: String = "connect.http.authentication.oauth2.client.headers.separator"
  val OAuth2ClientHeadersSeparatorDoc: String =
    s"""
       |The separator to use when splitting the ${OAuth2ClientHeadersProp} entries header-value pairs.
       |""".stripMargin
  val OAuth2ClientHeadersSeparatorDefault = ":"

  /**
    * Appends the OAuth2 ConfigDefs to the provided ConfigDef
    * @param config
    * @return
    */
  def append(config: ConfigDef): ConfigDef =
    config.define(
      OAuth2TokenUrlProp,
      Type.STRING,
      OAuth2TokenUrlDefault,
      Importance.HIGH,
      OAuth2TokenUrlDoc,
    )
      .define(
        OAuth2ClientIdProp,
        Type.STRING,
        OAuth2ClientIdDefault,
        Importance.HIGH,
        OAuth2ClientIdDoc,
      )
      .define(
        OAuth2ClientSecretProp,
        Type.PASSWORD,
        OAuth2ClientSecretDefault,
        Importance.HIGH,
        OAuth2ClientSecretDoc,
      )
      .define(
        OAuth2AccessTokenFieldProp,
        Type.STRING,
        OAuth2AccessTokenDefault,
        Importance.HIGH,
        OAuth2AccessTokenFieldDoc,
      )
      .define(
        OAuth2ClientScopeProp,
        Type.STRING,
        OAuth2ClientScopeDefault,
        Importance.HIGH,
        OAuth2ClientScopeDoc,
      )
      .define(
        OAuth2ClientHeadersProp,
        Type.LIST,
        List.empty.asJava,
        Importance.HIGH,
        OAuth2ClientHeadersDoc,
      )
      .define(
        OAuth2ClientHeadersSeparatorProp,
        Type.STRING,
        OAuth2ClientHeadersSeparatorDefault,
        Importance.HIGH,
        OAuth2ClientHeadersSeparatorDoc,
      )
}
