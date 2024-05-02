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
package io.lenses.streamreactor.connect.gcp.common.config;

import io.lenses.streamreactor.common.config.base.ConfigMap;
import io.lenses.streamreactor.common.config.base.RetryConfig;
import io.lenses.streamreactor.common.config.base.model.ConnectorPrefix;
import io.lenses.streamreactor.connect.gcp.common.auth.GCPConnectionConfig;
import io.lenses.streamreactor.connect.gcp.common.auth.HttpTimeoutConfig;
import lombok.Getter;
import lombok.val;
import org.apache.kafka.common.config.ConfigDef;

@Getter
public class GCPSettings {

  public static final String EMPTY_STRING = "";

  private final String gcpProjectId;
  private final String gcpQuotaProjectId;
  private final String host;
  private final String httpErrorRetryInterval;
  private final String httpNbrOfRetries;
  private final String httpSocketTimeout;
  private final String httpConnectionTimeout;


  public static final Long HTTP_ERROR_RETRY_INTERVAL_DEFAULT = 50L;
    public static final Integer HTTP_NUMBER_OF_RETIRES_DEFAULT = 5;
    public static final Long HTTP_SOCKET_TIMEOUT_DEFAULT = 60000L;
    public static final Integer HTTP_CONNECTION_TIMEOUT_DEFAULT = 60000;

  private final AuthModeSettings authModeSettings;

  public GCPSettings(ConnectorPrefix connectorPrefix) {
    gcpProjectId = connectorPrefix.prefixKey("gcp.project.id");
    gcpQuotaProjectId = connectorPrefix.prefixKey("gcp.quota.project.id");
    host = connectorPrefix.prefixKey("endpoint");
    httpErrorRetryInterval = connectorPrefix.prefixKey("http.retry.interval");
    httpNbrOfRetries = connectorPrefix.prefixKey("http.max.retries");
    httpSocketTimeout = connectorPrefix.prefixKey("http.socket.timeout");
    httpConnectionTimeout = connectorPrefix.prefixKey("http.connection.timeout");

    authModeSettings = new AuthModeSettings(connectorPrefix);
  }

  public ConfigDef withGCPSettings(ConfigDef configDef) {
      val conf = configDef.define(
                      gcpProjectId,
            ConfigDef.Type.STRING,
                      EMPTY_STRING,
            ConfigDef.Importance.HIGH,
            "GCP Project ID"
            )
            .define(
                    gcpQuotaProjectId,
                    ConfigDef.Type.STRING,
                    EMPTY_STRING,
                    ConfigDef.Importance.HIGH,
                    "GCP Quota Project ID"
                    )
            .define(
                    host,
                    ConfigDef.Type.STRING,
                    EMPTY_STRING,
                    ConfigDef.Importance.LOW,
                    "GCP Host"
                    )
            .define(
                    httpNbrOfRetries,
                    ConfigDef.Type.INT,
                    HTTP_NUMBER_OF_RETIRES_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    "Number of times to retry the http request, in the case of a resolvable error on the server side.",
                    "Error",
                    2,
                    ConfigDef.Width.LONG,
                    httpNbrOfRetries
                    )
              .define(
                      httpErrorRetryInterval,
                      ConfigDef.Type.LONG,
                      HTTP_ERROR_RETRY_INTERVAL_DEFAULT,
                      ConfigDef.Importance.MEDIUM,
                      "If greater than zero, used to determine the delay after which to retry the http request in milliseconds.  Based on an exponential backoff algorithm.",
                      "Error",
                      3,
                      ConfigDef.Width.LONG,
                      httpErrorRetryInterval
                      )
            .define(
                    httpSocketTimeout,
                    ConfigDef.Type.LONG,
                    HTTP_SOCKET_TIMEOUT_DEFAULT,
                    ConfigDef.Importance.LOW,
                    "Socket timeout (ms)"
                    )
            .define(
                    httpConnectionTimeout,
                    ConfigDef.Type.INT,
                    HTTP_CONNECTION_TIMEOUT_DEFAULT,
                    ConfigDef.Importance.LOW,
                    "Connection timeout (ms)"
                    );

    return authModeSettings.withAuthModeSettings(conf);

  }

  public GCPConnectionConfig parseFromConfig(ConfigMap configMap) {
      return new GCPConnectionConfig(
              configMap.getString(gcpProjectId).orElse(null),
              configMap.getString(gcpQuotaProjectId).orElse(null),
              authModeSettings.parseFromConfig(configMap),
              configMap.getString(host).orElse(null),
              new RetryConfig(
                      configMap.getInt(httpNbrOfRetries).orElse(HTTP_NUMBER_OF_RETIRES_DEFAULT),
                      configMap.getLong(httpErrorRetryInterval).orElse(HTTP_ERROR_RETRY_INTERVAL_DEFAULT)
                      ),
              new HttpTimeoutConfig(
                      configMap.getLong(httpSocketTimeout).orElse(null),
                      configMap.getLong(httpConnectionTimeout).orElse(null)
                      )
              );

  }
}
