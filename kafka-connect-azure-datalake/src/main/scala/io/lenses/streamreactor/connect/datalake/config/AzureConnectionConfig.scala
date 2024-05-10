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

import io.lenses.streamreactor.common.config.base.RetryConfig
import io.lenses.streamreactor.common.config.base.intf.ConnectionConfig
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse.getInt
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse.getLong
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse.getString
import io.lenses.streamreactor.connect.datalake.config.AzureConfigSettings._

object AzureConnectionConfig {

  def apply(props: Map[String, _], authMode: AuthMode): AzureConnectionConfig = AzureConnectionConfig(
    authMode,
    getString(props, ENDPOINT),
    new RetryConfig(
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

}

case class HttpTimeoutConfig(socketTimeout: Option[Long], connectionTimeout: Option[Long])

case class ConnectionPoolConfig(maxConnections: Int)

object ConnectionPoolConfig {
  def apply(maxConns: Option[Int]): Option[ConnectionPoolConfig] =
    maxConns.filterNot(_ == -1).map(ConnectionPoolConfig(_))
}

case class AzureConnectionConfig(
  authMode:             AuthMode,
  endpoint:             Option[String]               = None,
  httpRetryConfig:      RetryConfig                  = new RetryConfig(HTTP_NBR_OF_RETIRES_DEFAULT, HTTP_ERROR_RETRY_INTERVAL_DEFAULT),
  timeouts:             HttpTimeoutConfig            = HttpTimeoutConfig(None, None),
  connectionPoolConfig: Option[ConnectionPoolConfig] = Option.empty,
) extends ConnectionConfig
