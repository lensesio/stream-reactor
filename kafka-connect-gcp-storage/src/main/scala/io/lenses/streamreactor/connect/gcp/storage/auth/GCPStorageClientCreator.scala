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
package io.lenses.streamreactor.connect.gcp.storage.auth

import com.google.api.gax.retrying.RetrySettings
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.http.HttpTransportOptions
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.cloud.NoCredentials
import com.google.cloud.TransportOptions
import io.lenses.streamreactor.connect.cloud.common.auth.ClientCreator
import io.lenses.streamreactor.connect.gcp.storage.config.AuthMode.None
import io.lenses.streamreactor.connect.gcp.storage.config.AuthMode
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfig
import io.lenses.streamreactor.connect.gcp.storage.config.HttpTimeoutConfig
import io.lenses.streamreactor.connect.gcp.storage.config.RetryConfig
import org.threeten.bp.Duration

import java.io.ByteArrayInputStream
import java.io.FileInputStream
import scala.util.Try

object GCPStorageClientCreator extends ClientCreator[GCPConfig, Storage] {

  def make(config: GCPConfig): Either[Throwable, Storage] =
    Try {
      val builder = StorageOptions
        .newBuilder()

      config.host.foreach(builder.setHost)
      config.projectId.foreach(builder.setProjectId)
      config.quotaProjectId.foreach(builder.setQuotaProjectId)

      builder.setCredentials(
        config.authMode match {
          case None => NoCredentials.getInstance()
          case AuthMode.Credentials(credentials) =>
            GoogleCredentials.fromStream(new ByteArrayInputStream(credentials.value().getBytes))
          case AuthMode.File(filePath) => GoogleCredentials.fromStream(new FileInputStream(filePath))
          case _                       => GoogleCredentials.getApplicationDefault
        },
      )
        .setRetrySettings(createRetrySettings(config.httpRetryConfig))

      createTransportOptions(config.timeouts).foreach(builder.setTransportOptions)

      builder.build()
        .getService
    }.toEither

  private def createTransportOptions(timeoutConfig: HttpTimeoutConfig): Option[TransportOptions] =
    Option.when(timeoutConfig.connectionTimeout.nonEmpty || timeoutConfig.socketTimeout.nonEmpty) {
      val httpTransportOptionsBuilder = HttpTransportOptions.newBuilder()
      timeoutConfig.socketTimeout.foreach(sock => httpTransportOptionsBuilder.setReadTimeout(sock.toInt))
      timeoutConfig.connectionTimeout.foreach(conn => httpTransportOptionsBuilder.setConnectTimeout(conn.toInt))
      httpTransportOptionsBuilder.build()
    }

  private def createRetrySettings(httpRetryConfig: RetryConfig): RetrySettings =
    RetrySettings
      .newBuilder()
      .setInitialRetryDelay(Duration.ofMillis(httpRetryConfig.errorRetryInterval))
      .setMaxRetryDelay(Duration.ofMillis(httpRetryConfig.errorRetryInterval * 5))
      .setMaxAttempts(httpRetryConfig.numberOfRetries)
      .build()

}
