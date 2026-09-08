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
package io.lenses.streamreactor.connect.datalake.auth

import io.lenses.streamreactor.connect.datalake.config.AuthMode
import io.lenses.streamreactor.connect.datalake.config.AuthModeSettingsTest
import io.lenses.streamreactor.connect.datalake.config.AzureConfigSettings
import io.lenses.streamreactor.connect.datalake.config.AzureConnectionConfig
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DatalakeClientCreatorTest extends AnyFunSuite with Matchers with EitherValues {

  private def servicePrincipal(accountName: Option[String] = None): AuthMode.ServicePrincipal =
    AuthMode.ServicePrincipal(
      clientId     = "5c9a4b1e-1111-2222-3333-444455556666",
      tenantId     = "9f8e7d6c-aaaa-bbbb-cccc-ddddeeeeffff",
      clientSecret = new Password("not-a-real-secret"),
      accountName  = accountName,
    )

  test("service principal auth mode uses the explicitly configured endpoint") {
    val config = AzureConnectionConfig(
      authMode = servicePrincipal(),
      endpoint = Some("https://myaccount.dfs.core.windows.net"),
    )

    DatalakeClientCreator.make(config).value.getAccountUrl should be("https://myaccount.dfs.core.windows.net")
  }

  test("service principal auth mode derives the endpoint from the account name") {
    val config = AzureConnectionConfig(authMode = servicePrincipal(Some("myaccount")))

    DatalakeClientCreator.make(config).value.getAccountUrl should be("https://myaccount.dfs.core.windows.net")
  }

  test("service principal auth mode prefers the endpoint over the account name") {
    val config = AzureConnectionConfig(
      authMode = servicePrincipal(Some("myaccount")),
      endpoint = Some("https://otheraccount.dfs.core.windows.net"),
    )

    DatalakeClientCreator.make(config).value.getAccountUrl should be("https://otheraccount.dfs.core.windows.net")
  }

  test("service principal auth mode falls back to the account name when the endpoint is blank") {
    val config = AzureConnectionConfig(authMode = servicePrincipal(Some("myaccount")), endpoint = Some("   "))

    DatalakeClientCreator.make(config).value.getAccountUrl should be("https://myaccount.dfs.core.windows.net")
  }

  test("service principal auth mode fails when neither endpoint nor account name is configured") {
    val error = DatalakeClientCreator.make(AzureConnectionConfig(authMode = servicePrincipal())).left.value

    error shouldBe a[ConfigException]
    error.getMessage should include(AzureConfigSettings.ENDPOINT)
    error.getMessage should include(AuthModeSettingsTest.Keys.accountName)
    error.getMessage should include(AuthModeSettingsTest.Keys.authMode)
  }

  test("creates a client for the shared key auth mode") {
    val config = AzureConnectionConfig(
      authMode = AuthMode.Credentials("myaccount", new Password("not-a-real-secret")),
      endpoint = Some("https://myaccount.dfs.core.windows.net"),
    )

    DatalakeClientCreator.make(config).value.getAccountUrl should be("https://myaccount.dfs.core.windows.net")
  }
}
