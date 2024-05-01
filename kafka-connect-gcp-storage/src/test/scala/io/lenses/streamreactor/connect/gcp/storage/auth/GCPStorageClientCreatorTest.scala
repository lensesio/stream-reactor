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
package io.lenses.streamreactor.connect.gcp.storage.auth

import com.google.cloud.TransportOptions
import com.google.cloud.http.HttpTransportOptions
import io.lenses.streamreactor.common.config.base.RetryConfig
import io.lenses.streamreactor.connect.gcp.common.auth.mode.CredentialsAuthMode
import io.lenses.streamreactor.connect.gcp.common.auth.mode.DefaultAuthMode
import io.lenses.streamreactor.connect.gcp.common.auth.mode.FileAuthMode
import io.lenses.streamreactor.connect.gcp.common.auth.mode.NoAuthMode
import io.lenses.streamreactor.connect.gcp.common.auth.GCPConnectionConfig
import io.lenses.streamreactor.connect.gcp.common.auth.HttpTimeoutConfig
import org.apache.commons.io.IOUtils
import org.apache.kafka.common.config.types.Password
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.threeten.bp.Duration

import java.net.URL
import java.nio.charset.Charset

class GCPStorageClientCreatorTest extends AnyFunSuite with Matchers with EitherValues {

  private val jsonCredsUrl: URL = getClass.getResource("/test-gcp-credentials.json")

  private val defaultConfigBuilder = GCPConnectionConfig.builder()
    .host("custom-host")
    .projectId("project-id")
    .quotaProjectId("quota-project-id")
    .authMode(new NoAuthMode());

  test("should provide specified base options") {
    val config = defaultConfigBuilder.build()

    val storageEither = GCPStorageClientCreator.make(config)

    val storageOptions = storageEither.value.getOptions
    storageOptions.getHost shouldBe "custom-host"
    storageOptions.getProjectId shouldBe "project-id"
    storageOptions.getQuotaProjectId shouldBe "quota-project-id"
  }

  test("should handle AuthMode.None") {
    val config = defaultConfigBuilder.build()

    GCPStorageClientCreator.make(config).value.getOptions.getCredentials.getClass.getSimpleName should be(
      "NoCredentials",
    )
  }

  test("should handle AuthMode.Default") {
    val config = defaultConfigBuilder.authMode(new DefaultAuthMode()).build()

    // we probably don't have GCP credentials configured so we would expect this to fail.
    GCPStorageClientCreator.make(config).swap.value.getMessage should startWith(
      "Your default credentials were not found",
    )
  }

  test("should handle AuthMode.Credentials") {
    val testCreds = IOUtils.toString(jsonCredsUrl, Charset.defaultCharset())
    val config    = defaultConfigBuilder.authMode(new CredentialsAuthMode(new Password(testCreds))).build()

    val storageEither = GCPStorageClientCreator.make(config)

    storageEither.isRight shouldBe true
    storageEither.value.getOptions.getCredentials.getClass.getSimpleName should be("ServiceAccountCredentials")
  }

  test("should handle AuthMode.File") {
    val filePath = jsonCredsUrl.getPath
    val config   = defaultConfigBuilder.authMode(new FileAuthMode(filePath)).build()

    val storageEither = GCPStorageClientCreator.make(config)

    storageEither.isRight shouldBe true
    storageEither.value.getOptions.getCredentials.getClass.getSimpleName should be("ServiceAccountCredentials")
  }

  test("should handle http retry config") {

    val config = defaultConfigBuilder
      .httpRetryConfig(new RetryConfig(100, 500)).build()

    val retrySettings = GCPStorageClientCreator.make(config).value.getOptions.getRetrySettings
    retrySettings.getMaxAttempts should be(100)
    retrySettings.getInitialRetryDelay should be(Duration.ofMillis(500))

  }

  test("should use http by default") {

    val config = defaultConfigBuilder.build()

    val transportOpts: TransportOptions = GCPStorageClientCreator.make(config).value.getOptions.getTransportOptions
    transportOpts match {
      case _: HttpTransportOptions =>
      case _ => fail("Nope")
    }
  }

  test("should handle http timeout config") {

    val config = defaultConfigBuilder.timeouts(
      new HttpTimeoutConfig(250L, 800L),
    ).build()

    val transportOpts: TransportOptions = GCPStorageClientCreator.make(config).value.getOptions.getTransportOptions
    transportOpts match {
      case options: HttpTransportOptions =>
        options.getReadTimeout should be(250)
        options.getConnectTimeout should be(800)
      case _ => fail("Nope")
    }
  }

}
