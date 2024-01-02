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
package io.lenses.streamreactor.connect.datalake.config

import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.common.errors.ErrorPolicyEnum
import io.lenses.streamreactor.common.errors.ThrowErrorPolicy
import io.lenses.streamreactor.connect.cloud.common.config.CloudConfig
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse.getInt
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse.getLong
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse.getString
import io.lenses.streamreactor.connect.datalake.config.AzureConfigSettings._

object AzureConfig {

  def apply(props: Map[String, _], authMode: AuthMode): AzureConfig = AzureConfig(
    authMode,
    getString(props, ENDPOINT),
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
    ConnectionPoolConfig(
      getInt(props, POOL_MAX_CONNECTIONS),
    ),
  )

  private def getErrorPolicy(props: Map[String, _]) =
    ErrorPolicy(
      ErrorPolicyEnum.withName(getString(props, ERROR_POLICY).map(_.toUpperCase()).getOrElse(ERROR_POLICY_DEFAULT)),
    )
}

case class RetryConfig(numberOfRetries: Int, errorRetryInterval: Long)

case class HttpTimeoutConfig(socketTimeout: Option[Long], connectionTimeout: Option[Long])

case class ConnectionPoolConfig(maxConnections: Int)

object ConnectionPoolConfig {
  def apply(maxConns: Option[Int]): Option[ConnectionPoolConfig] =
    maxConns.filterNot(_ == -1).map(ConnectionPoolConfig(_))
}

case class AzureConfig(
  authMode:             AuthMode,
  endpoint:             Option[String]               = None,
  errorPolicy:          ErrorPolicy                  = ThrowErrorPolicy(),
  connectorRetryConfig: RetryConfig                  = RetryConfig(NBR_OF_RETIRES_DEFAULT, ERROR_RETRY_INTERVAL_DEFAULT),
  httpRetryConfig:      RetryConfig                  = RetryConfig(HTTP_NBR_OF_RETIRES_DEFAULT, HTTP_ERROR_RETRY_INTERVAL_DEFAULT),
  timeouts:             HttpTimeoutConfig            = HttpTimeoutConfig(None, None),
  connectionPoolConfig: Option[ConnectionPoolConfig] = Option.empty,
) extends CloudConfig
