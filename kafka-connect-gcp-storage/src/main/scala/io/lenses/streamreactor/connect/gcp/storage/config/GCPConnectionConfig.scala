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
package io.lenses.streamreactor.connect.gcp.storage.config

import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.common.errors.ErrorPolicyEnum
import io.lenses.streamreactor.common.errors.ThrowErrorPolicy
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse._
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse.getString
import GCPConfigSettings.ERROR_POLICY
import GCPConfigSettings.ERROR_POLICY_DEFAULT
import GCPConfigSettings.ERROR_RETRY_INTERVAL
import GCPConfigSettings.ERROR_RETRY_INTERVAL_DEFAULT
import GCPConfigSettings.GCP_PROJECT_ID
import GCPConfigSettings.GCP_QUOTA_PROJECT_ID
import GCPConfigSettings.HOST
import GCPConfigSettings.HTTP_CONNECTION_TIMEOUT
import GCPConfigSettings.HTTP_ERROR_RETRY_INTERVAL
import GCPConfigSettings.HTTP_ERROR_RETRY_INTERVAL_DEFAULT
import GCPConfigSettings.HTTP_NBR_OF_RETIRES_DEFAULT
import GCPConfigSettings.HTTP_NBR_OF_RETRIES
import GCPConfigSettings.HTTP_SOCKET_TIMEOUT
import GCPConfigSettings.NBR_OF_RETIRES_DEFAULT
import GCPConfigSettings.NBR_OF_RETRIES
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudConnectionConfig

object GCPConnectionConfig {

  def apply(props: Map[String, _], authMode: AuthMode): GCPConnectionConfig = GCPConnectionConfig(
    getString(props, GCP_PROJECT_ID),
    getString(props, GCP_QUOTA_PROJECT_ID),
    authMode,
    getString(props, HOST),
    getErrorPolicy(props),
    RetryConfig(
      getInt(props, NBR_OF_RETRIES).getOrElse(NBR_OF_RETIRES_DEFAULT),
      getLong(props, ERROR_RETRY_INTERVAL).getOrElse(ERROR_RETRY_INTERVAL_DEFAULT),
    ),
    RetryConfig(
      getInt(props, HTTP_NBR_OF_RETRIES).getOrElse(HTTP_NBR_OF_RETIRES_DEFAULT),
      getLong(props, HTTP_ERROR_RETRY_INTERVAL).getOrElse(HTTP_ERROR_RETRY_INTERVAL_DEFAULT),
    ),
    HttpTimeoutConfig(
      getLong(props, HTTP_SOCKET_TIMEOUT),
      getLong(props, HTTP_CONNECTION_TIMEOUT),
    ),
  )

  private def getErrorPolicy(props: Map[String, _]) =
    ErrorPolicy(
      ErrorPolicyEnum.withName(getString(props, ERROR_POLICY).map(_.toUpperCase()).getOrElse(ERROR_POLICY_DEFAULT)),
    )
}

case class RetryConfig(numberOfRetries: Int, errorRetryInterval: Long)

case class HttpTimeoutConfig(socketTimeout: Option[Long], connectionTimeout: Option[Long])

case class GCPConnectionConfig(
  projectId:            Option[String],
  quotaProjectId:       Option[String],
  authMode:             AuthMode,
  host:                 Option[String]    = None,
  errorPolicy:          ErrorPolicy       = ThrowErrorPolicy(),
  connectorRetryConfig: RetryConfig       = RetryConfig(NBR_OF_RETIRES_DEFAULT, ERROR_RETRY_INTERVAL_DEFAULT),
  httpRetryConfig:      RetryConfig       = RetryConfig(HTTP_NBR_OF_RETIRES_DEFAULT, HTTP_ERROR_RETRY_INTERVAL_DEFAULT),
  timeouts:             HttpTimeoutConfig = HttpTimeoutConfig(None, None),
) extends CloudConnectionConfig
