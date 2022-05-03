/*
 * Copyright 2017 Datamountaineer.
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

package com.datamountaineer.streamreactor.connect.hazelcast.config

import org.apache.kafka.common.config.SslConfigs

import scala.jdk.CollectionConverters.ListHasAsScala


/**
  * Created by andrew@datamountaineer.com on 10/08/16. 
  * stream-reactor
  */
case class HazelCastConnectionConfig(clusterName: String,
                                     members: Set[String],
                                     redo: Boolean = true,
                                     connectionAttempts: Int,
                                     connectionTimeouts: Long,
                                     socketConfig: HazelCastSocketConfig,
                                     sslEnabled: Boolean = false,
                                     trustStoreType: Option[String] = None,
                                     trustStorePassword: Option[String] = None,
                                     trustStoreLocation: Option[String] = None,
                                     keyStoreType: Option[String] = None,
                                     keyStorePassword: Option[String] = None,
                                     keyStoreLocation: Option[String] = None
                                    )

case class HazelCastSocketConfig(keepAlive: Boolean = true,
                                 tcpNoDelay: Boolean = true,
                                 reuseAddress: Boolean = true,
                                 lingerSeconds: Int = 3,
                                 bufferSize: Int = 32)


object HazelCastConnectionConfig {
  def apply(config: HazelCastSinkConfig): HazelCastConnectionConfig = {
    val members = config.getList(HazelCastSinkConfigConstants.CLUSTER_MEMBERS).asScala.toSet
    val redo = true
    val connectionAttempts = config.getInt(HazelCastSinkConfigConstants.CONNECTION_RETRY_ATTEMPTS)
    val connectionTimeouts = config.getLong(HazelCastSinkConfigConstants.CONNECTION_TIMEOUT)
    val keepAlive = config.getBoolean(HazelCastSinkConfigConstants.KEEP_ALIVE)
    val tcpNoDelay = config.getBoolean(HazelCastSinkConfigConstants.TCP_NO_DELAY)
    val reuse = config.getBoolean(HazelCastSinkConfigConstants.REUSE_ADDRESS)
    val linger = config.getInt(HazelCastSinkConfigConstants.LINGER_SECONDS)
    val buffer = config.getInt(HazelCastSinkConfigConstants.BUFFER_SIZE)
    val socketConfig = HazelCastSocketConfig(keepAlive, tcpNoDelay, reuse, linger, buffer)
    val clusterName = config.getString(HazelCastSinkConfigConstants.CLUSTER_NAME)
    val ssl = config.getBoolean(HazelCastSinkConfigConstants.SSL_ENABLED)

    val trustStoreType = Option(config.getString(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG))
    val trustStorePath = Option(config.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG))
    val trustStorePassword = Option(config.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)) match {
      case Some(p) => Some(p.value())
      case None => None
    }

    val keyStoreType = Option(config.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG))
    val keyStorePath = Option(config.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
    val keyStorePassword = Option(config.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)) match {
      case Some(p) => Some(p.value())
      case None => None
    }

    new HazelCastConnectionConfig(
      clusterName,
      members,
      redo,
      connectionAttempts,
      connectionTimeouts,
      socketConfig,
      ssl,
      trustStoreType,
      trustStorePassword,
      trustStorePath,
      keyStoreType,
      keyStorePassword,
      keyStorePath
    )
  }
}
