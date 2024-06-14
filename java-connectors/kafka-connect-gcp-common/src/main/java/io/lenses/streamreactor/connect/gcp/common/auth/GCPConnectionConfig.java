/*
 * Copyright 2017-2024 Lenses.io Ltd
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

import static io.lenses.streamreactor.connect.gcp.common.config.GCPSettings.HTTP_ERROR_RETRY_INTERVAL_DEFAULT;
import static io.lenses.streamreactor.connect.gcp.common.config.GCPSettings.HTTP_NUMBER_OF_RETIRES_DEFAULT;

import java.util.Optional;

import io.lenses.streamreactor.common.config.base.RetryConfig;
import io.lenses.streamreactor.common.config.base.intf.ConnectionConfig;
import io.lenses.streamreactor.connect.gcp.common.auth.mode.AuthMode;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class GCPConnectionConfig implements ConnectionConfig {

  private String projectId;
  private String quotaProjectId;
  private AuthMode authMode;
  private String host;

  @Builder.Default
  private RetryConfig httpRetryConfig =
      RetryConfig.builder()
          .retryLimit(HTTP_NUMBER_OF_RETIRES_DEFAULT)
          .retryIntervalMillis(HTTP_ERROR_RETRY_INTERVAL_DEFAULT)
          .build();

  @Builder.Default
  private HttpTimeoutConfig timeouts = HttpTimeoutConfig.builder().build();

  public Optional<AuthMode> getAuthMode() {
    return Optional.ofNullable(authMode);
  }

  public Optional<RetryConfig> getHttpRetryConfig() {
    return Optional.ofNullable(httpRetryConfig);
  }

  public Optional<HttpTimeoutConfig> getTimeouts() {
    return Optional.ofNullable(timeouts);
  }
}
