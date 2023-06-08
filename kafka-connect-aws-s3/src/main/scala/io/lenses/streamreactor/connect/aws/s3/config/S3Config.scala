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
package io.lenses.streamreactor.connect.aws.s3.config

import com.datamountaineer.streamreactor.common.errors.ErrorPolicy
import com.datamountaineer.streamreactor.common.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.common.errors.ThrowErrorPolicy
import enumeratum.Enum
import enumeratum.EnumEntry
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import org.apache.kafka.common.config.types.Password

import scala.collection.immutable

sealed trait AuthMode extends EnumEntry

object AuthMode extends Enum[AuthMode] {

  override val values: immutable.IndexedSeq[AuthMode] = findValues

  case object Credentials extends AuthMode

  case object Default extends AuthMode

}

object S3Config {

  def getString(props: Map[String, _], key: String): Option[String] =
    props.get(key)
      .collect {
        case s: String if s.nonEmpty => s
      }

  def getPassword(props: Map[String, _], key: String): Option[String] =
    props.get(key)
      .collect {
        case p: Password if p.value().nonEmpty => p.value()
        case s: String if s.nonEmpty           => s
      }

  def getBoolean(props: Map[String, _], key: String): Option[Boolean] =
    props.get(key)
      .collect {
        case b: Boolean => b
        case "true"  => true
        case "false" => false
      }

  def getLong(props: Map[String, _], key: String): Option[Long] =
    props.get(key)
      .collect {
        case i: Int    => i.toLong
        case l: Long   => l
        case s: String => s.toLong
      }

  def getInt(props: Map[String, _], key: String): Option[Int] =
    props.get(key)
      .collect {
        case i: Int    => i
        case l: Long   => l.toInt
        case i: String => i.toInt
      }

  def apply(props: Map[String, _]): S3Config = S3Config(
    getString(props, AWS_REGION),
    getPassword(props, AWS_ACCESS_KEY),
    getPassword(props, AWS_SECRET_KEY),
    AuthMode.withNameInsensitive(
      getString(props, AUTH_MODE).getOrElse(AuthMode.Default.toString),
    ),
    getString(props, CUSTOM_ENDPOINT),
    getBoolean(props, ENABLE_VIRTUAL_HOST_BUCKETS).getOrElse(false),
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
      getInt(props, HTTP_SOCKET_TIMEOUT),
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

case class HttpTimeoutConfig(socketTimeout: Option[Int], connectionTimeout: Option[Long])

case class ConnectionPoolConfig(maxConnections: Int)

object ConnectionPoolConfig {
  def apply(maxConns: Option[Int]): Option[ConnectionPoolConfig] =
    maxConns.filterNot(_ == -1).map(ConnectionPoolConfig(_))
}

case class S3Config(
  region:                   Option[String],
  accessKey:                Option[String],
  secretKey:                Option[String],
  authMode:                 AuthMode,
  customEndpoint:           Option[String]               = None,
  enableVirtualHostBuckets: Boolean                      = false,
  errorPolicy:              ErrorPolicy                  = ThrowErrorPolicy(),
  connectorRetryConfig:     RetryConfig                  = RetryConfig(NBR_OF_RETIRES_DEFAULT, ERROR_RETRY_INTERVAL_DEFAULT),
  httpRetryConfig:          RetryConfig                  = RetryConfig(HTTP_NBR_OF_RETIRES_DEFAULT, HTTP_ERROR_RETRY_INTERVAL_DEFAULT),
  timeouts:                 HttpTimeoutConfig            = HttpTimeoutConfig(None, None),
  connectionPoolConfig:     Option[ConnectionPoolConfig] = Option.empty,
)
