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
package io.lenses.streamreactor.connect.datalake.config

import io.lenses.streamreactor.common.config.base.traits.BaseConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object AuthModeSettingsTest {

  object Keys extends AuthModeSettingsConfigKeys {
    override def connectorPrefix: String = AzureConfigSettings.CONNECTOR_PREFIX

    val configDef: ConfigDef = withAuthModeSettings(new ConfigDef())

    val authMode:         String = AUTH_MODE
    val accountName:      String = AZURE_ACCOUNT_NAME
    val accountKey:       String = AZURE_ACCOUNT_KEY
    val connectionString: String = AZURE_CONNECTION_STRING
    val clientId:         String = AZURE_CLIENT_ID
    val tenantId:         String = AZURE_TENANT_ID
    val clientSecret:     String = AZURE_CLIENT_SECRET
  }

  class TestSettings(props: Map[String, AnyRef])
    extends BaseConfig(AzureConfigSettings.CONNECTOR_PREFIX, Keys.configDef, props)
      with AuthModeSettings
}

class AuthModeSettingsTest extends AnyFunSuite with Matchers with EitherValues {

  import AuthModeSettingsTest._

  private val ClientIdValue     = "5c9a4b1e-1111-2222-3333-444455556666"
  private val TenantIdValue     = "9f8e7d6c-aaaa-bbbb-cccc-ddddeeeeffff"
  private val ClientSecretValue = "super-secret-value"

  private def servicePrincipalProps(extra: (String, AnyRef)*): Map[String, AnyRef] =
    Map[String, AnyRef](
      Keys.authMode     -> "serviceprincipal",
      Keys.clientId     -> ClientIdValue,
      Keys.tenantId     -> TenantIdValue,
      Keys.clientSecret -> ClientSecretValue,
    ) ++ extra

  private def authMode(props: Map[String, AnyRef]) = new TestSettings(props).getAuthMode

  test("parses a service principal auth mode without an account name") {
    val result = authMode(servicePrincipalProps()).value

    result shouldBe a[AuthMode.ServicePrincipal]
    val sp = result.asInstanceOf[AuthMode.ServicePrincipal]
    sp.clientId should be(ClientIdValue)
    sp.tenantId should be(TenantIdValue)
    sp.clientSecret.value() should be(ClientSecretValue)
    sp.accountName should be(None)
  }

  test("parses a service principal auth mode with an account name") {
    val result = authMode(servicePrincipalProps(Keys.accountName -> "mystorageaccount")).value

    result.asInstanceOf[AuthMode.ServicePrincipal].accountName should be(Some("mystorageaccount"))
  }

  test("ignores a blank account name for the service principal auth mode") {
    val result = authMode(servicePrincipalProps(Keys.accountName -> "   ")).value

    result.asInstanceOf[AuthMode.ServicePrincipal].accountName should be(None)
  }

  test("accepts the service principal auth mode irrespective of case and surrounding whitespace") {
    val result = authMode(servicePrincipalProps(Keys.authMode -> "  ServicePrincipal ")).value

    result shouldBe a[AuthMode.ServicePrincipal]
  }

  test("parses the credentials auth mode") {
    val result = authMode(
      Map[String, AnyRef](
        Keys.authMode    -> "credentials",
        Keys.accountName -> "mystorageaccount",
        Keys.accountKey  -> "account-key",
      ),
    ).value

    result shouldBe a[AuthMode.Credentials]
    result.asInstanceOf[AuthMode.Credentials].accountName should be("mystorageaccount")
  }

  test("parses the connection string auth mode") {
    val result = authMode(
      Map[String, AnyRef](
        Keys.authMode         -> "connectionstring",
        Keys.connectionString -> "connection-string",
      ),
    ).value

    result should be(AuthMode.ConnectionString("connection-string"))
  }

  test("still defaults to the default auth mode") {
    authMode(Map.empty[String, AnyRef]).value should be(AuthMode.Default)
    authMode(Map[String, AnyRef](Keys.authMode -> "default")).value should be(AuthMode.Default)
  }

  test("rejects an unsupported auth mode") {
    val error = authMode(Map[String, AnyRef](Keys.authMode -> "unsupported")).left.value

    error shouldBe a[ConfigException]
    error.getMessage should include("unsupported")
  }
}
