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
package io.lenses.streamreactor.connect.aws.s3.config

import enumeratum.Enum
import enumeratum.EnumEntry
import io.lenses.streamreactor.common.config.base.RetryConfig
import io.lenses.streamreactor.common.config.base.intf.ConnectionConfig
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse.getBoolean
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse.getInt
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse.getLong
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse.getPassword
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse.getString

import scala.collection.immutable

sealed trait AuthMode extends EnumEntry

object AuthMode extends Enum[AuthMode] {

  override val values: immutable.IndexedSeq[AuthMode] = findValues

  case object Credentials extends AuthMode

  case object Default extends AuthMode

}

object S3ConnectionConfig {

  def apply(props: Map[String, _]): S3ConnectionConfig = S3ConnectionConfig(
    getString(props, AWS_REGION),
    getPassword(props, AWS_ACCESS_KEY),
    getPassword(props, AWS_SECRET_KEY),
    AuthMode.withNameInsensitive(
      getString(props, AUTH_MODE).getOrElse(AuthMode.Default.toString),
    ),
    getString(props, CUSTOM_ENDPOINT),
    getBoolean(props, ENABLE_VIRTUAL_HOST_BUCKETS).getOrElse(false),
    new RetryConfig(
      getInt(props, HTTP_NBR_OF_RETRIES).getOrElse(HTTP_NBR_OF_RETIRES_DEFAULT),
      getLong(props, HTTP_ERROR_RETRY_INTERVAL).getOrElse(HTTP_ERROR_RETRY_INTERVAL_DEFAULT),
    ),
    HttpTimeoutConfig(
      getInt(props, HTTP_SOCKET_TIMEOUT),
      getLong(props, HTTP_CONNECTION_TIMEOUT),
    ),
    ConnectionPoolConfig(
      getInt(props, POOL_MAX_CONNECTIONS),
    ),
  )
}

case class HttpTimeoutConfig(socketTimeout: Option[Int], connectionTimeout: Option[Long])

case class ConnectionPoolConfig(maxConnections: Int)

object ConnectionPoolConfig {
  def apply(maxConns: Option[Int]): Option[ConnectionPoolConfig] =
    maxConns.filterNot(_ == -1).map(ConnectionPoolConfig(_))
}

case class S3ConnectionConfig(
  region:                   Option[String],
  accessKey:                Option[String],
  secretKey:                Option[String],
  authMode:                 AuthMode,
  customEndpoint:           Option[String]               = None,
  enableVirtualHostBuckets: Boolean                      = false,
  httpRetryConfig:          RetryConfig                  = new RetryConfig(HTTP_NBR_OF_RETIRES_DEFAULT, HTTP_ERROR_RETRY_INTERVAL_DEFAULT),
  timeouts:                 HttpTimeoutConfig            = HttpTimeoutConfig(None, None),
  connectionPoolConfig:     Option[ConnectionPoolConfig] = Option.empty,
) extends ConnectionConfig
