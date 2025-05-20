/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.gcp.common.auth;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.kafka.common.config.ConfigException;
import org.threeten.bp.Duration;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.Service;
import com.google.cloud.ServiceOptions;

import io.lenses.streamreactor.common.config.base.RetryConfig;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.val;

/**
 * Utility class for configuring generic GCP service clients using a {@link GCPConnectionConfig}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GCPServiceBuilderConfigurer {

  /**
   * Configures a GCP service client builder with the provided {@link GCPConnectionConfig}.
   *
   * @param <X>     Type representing the GCP service interface (e.g., Storage, BigQuery)
   * @param <Y>     Type representing the service options (e.g., StorageOptions, BigQueryOptions)
   * @param <B>     Type representing the service options builder (e.g., StorageOptions.Builder,
   *                BigQueryOptions.Builder)
   * @param config  The GCP connection configuration containing settings such as host, project ID, and authentication
   *                details.
   * @param builder The builder instance of the GCP service client options.
   * @return The configured builder instance with updated settings.
   * @throws IOException if an error occurs during configuration, such as credential retrieval.
   */
  public static <X extends Service<Y>, Y extends ServiceOptions<X, Y>, B extends ServiceOptions.Builder<X, Y, B>> B configure(
      GCPConnectionConfig config, B builder) throws IOException {

    Optional.ofNullable(config.getHost()).ifPresent(builder::setHost);

    Optional.ofNullable(config.getProjectId()).ifPresent(builder::setProjectId);

    Optional.ofNullable(config.getQuotaProjectId()).ifPresent(builder::setQuotaProjectId);

    val authMode =
        config.getAuthMode()
            .orElseThrow(createConfigException("AuthMode has to be configured by setting x.y.z property"));

    builder.setCredentials(authMode.getCredentials());

    builder.setRetrySettings(createRetrySettings(
        config.getHttpRetryConfig()
            .orElseThrow(createConfigException("RetrySettings has to be configured by setting a.b"))));

    return builder;
  }

  private static RetrySettings createRetrySettings(RetryConfig httpRetryConfig) {

    return RetrySettings.newBuilder()
        .setInitialRetryDelay(Duration.ofMillis(httpRetryConfig.getRetryIntervalMillis()))
        .setMaxRetryDelay(Duration.ofMillis(httpRetryConfig.getRetryIntervalMillis() * 5L))
        .setRetryDelayMultiplier(httpRetryConfig.getRetryDelayMultiplier())
        .setMaxAttempts(httpRetryConfig.getRetryLimit())
        .build();
  }

  private static Supplier<ConfigException> createConfigException(String message) {
    return () -> new ConfigException(message);
  }
}
