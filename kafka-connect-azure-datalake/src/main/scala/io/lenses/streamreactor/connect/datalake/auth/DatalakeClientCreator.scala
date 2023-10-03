/*
 * Copyright 2017-2023 Lenses.io Ltd
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

import com.azure.core.http.HttpClient
import com.azure.core.util.Configuration
import com.azure.core.util.ConfigurationBuilder
import com.azure.core.util.HttpClientOptions
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.common.StorageSharedKeyCredential
import com.azure.storage.file.datalake.DataLakeServiceClient
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder
import io.lenses.streamreactor.connect.cloud.common.auth.ClientCreator
import io.lenses.streamreactor.connect.datalake.config.AuthMode
import io.lenses.streamreactor.connect.datalake.config.AuthMode.ConnectionString
import io.lenses.streamreactor.connect.datalake.config.AuthMode.Credentials
import io.lenses.streamreactor.connect.datalake.config.AzureConfig
import io.lenses.streamreactor.connect.datalake.config.ConnectionPoolConfig

import java.time.Duration
import scala.util.Try

object DatalakeClientCreator extends ClientCreator[AzureConfig, DataLakeServiceClient] {

  def make(config: AzureConfig): Either[Throwable, DataLakeServiceClient] = {
    require(config != null, "AzureDataLakeConfig cannot be null")

    config.authMode match {
      case csam: AuthMode.ConnectionString =>
        createDataLakeClientWithConnectionString(csam)
      case cam: AuthMode.Credentials =>
        createDataLakeClientWithSharedKey(config, cam)
      case AuthMode.Default =>
        createDataLakeClientWithDefaultCredential(config)
      case _ =>
        Left(new IllegalArgumentException(s"Unsupported authentication mode: ${config.authMode}"))
    }
  }

  private def createHttpClient(config: AzureConfig): HttpClient = {

    val httpClientOptions = new HttpClientOptions()
    config.timeouts.socketTimeout.foreach(millis => httpClientOptions.setConnectTimeout(Duration.ofMillis(millis)))
    config.timeouts.connectionTimeout.foreach(millis =>
      httpClientOptions.setConnectionIdleTimeout(Duration.ofMillis(millis)),
    )
    config.connectionPoolConfig.foreach { cpc: ConnectionPoolConfig =>
      httpClientOptions.setMaximumConnectionPoolSize(cpc.maxConnections)
    }
    val configuration = new ConfigurationBuilder().putProperty(Configuration.PROPERTY_AZURE_REQUEST_RETRY_COUNT,
                                                               config.httpRetryConfig.numberOfRetries.toString,
    ).build()
    httpClientOptions.setConfiguration(configuration)
    HttpClient.createDefault(httpClientOptions)
  }

  private def createDataLakeClientWithConnectionString(
    authMode: ConnectionString,
  ): Either[Throwable, DataLakeServiceClient] =
    Try {
      val builder = new DataLakeServiceClientBuilder()
        .connectionString(authMode.connectionString)

      builder.buildClient()

    }.toEither

  private def createDataLakeClientWithSharedKey(
    config:   AzureConfig,
    authMode: Credentials,
  ): Either[Throwable, DataLakeServiceClient] =
    Try {
      val storageCredentials = new StorageSharedKeyCredential(authMode.accountName, authMode.accountKey.value())

      val builder = new DataLakeServiceClientBuilder()
        .credential(storageCredentials)
        .httpClient(createHttpClient(config))

      config.endpoint.foreach(builder.endpoint)

      builder.buildClient()

    }.toEither

  private def createDataLakeClientWithDefaultCredential(config: AzureConfig): Either[Throwable, DataLakeServiceClient] =
    Try {
      val builder = new DataLakeServiceClientBuilder()
        .credential(new DefaultAzureCredentialBuilder().build())
        .httpClient(createHttpClient(config))

      config.endpoint.foreach(builder.endpoint)

      builder.buildClient()
    }.toEither
}
