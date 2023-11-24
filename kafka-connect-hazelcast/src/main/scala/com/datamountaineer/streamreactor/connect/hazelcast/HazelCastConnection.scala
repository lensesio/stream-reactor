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
package io.lenses.streamreactor.connect.hazelcast

import java.io.File
import java.io.FileNotFoundException
import java.net.URI
import java.util.Properties
import java.util.UUID
import io.lenses.streamreactor.connect.hazelcast.config.HazelCastConnectionConfig
import io.lenses.streamreactor.connect.hazelcast.config.HazelCastSocketConfig
import com.hazelcast.cache.HazelcastCachingProvider
import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.ClientConfig
import com.hazelcast.client.config.ClientNetworkConfig
import com.hazelcast.client.config.SocketOptions
import com.hazelcast.config.SSLConfig
import com.hazelcast.core.HazelcastInstance

import javax.cache.CacheManager
import javax.cache.Caching
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
  * Created by andrew@datamountaineer.com on 10/08/16.
  * stream-reactor
  */
object HazelCastConnection {
  def buildClient(config: HazelCastConnectionConfig): HazelcastInstance = {
    val clientConfig  = new ClientConfig
    val networkConfig = clientConfig.getNetworkConfig

    if (config.sslEnabled) {
      setSSLOptions(config)
      networkConfig.setSSLConfig(new SSLConfig().setEnabled(true))
    }
    networkConfig.setAddresses(config.members.toList.asJava)

    clientConfig.setClusterName(config.clusterName)

    buildSocketOptions(networkConfig, config.socketConfig)
    clientConfig.setInstanceName(config.clusterName + "-kafka-connect-" + UUID.randomUUID().toString)
    HazelcastClient.newHazelcastClient(clientConfig)
  }

  private def buildSocketOptions(
    clientNetworkConfig: ClientNetworkConfig,
    socketConfig:        HazelCastSocketConfig,
  ): SocketOptions = {
    val socketOptions = clientNetworkConfig.getSocketOptions
    socketOptions.setKeepAlive(socketConfig.keepAlive)
    socketOptions.setTcpNoDelay(socketConfig.tcpNoDelay)
    socketOptions.setReuseAddress(socketConfig.reuseAddress)
    socketOptions.setLingerSeconds(socketConfig.lingerSeconds)
    socketOptions.setBufferSize(socketConfig.bufferSize)
    socketOptions
  }

  def getCacheManager(client: HazelcastInstance, name: String): CacheManager = {
    val instanceName    = client.getName()
    val cachingProvider = Caching.getCachingProvider()

    // Create Properties instance pointing to a named HazelcastInstance
    val properties = new Properties()
    properties.setProperty(HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME, instanceName)
    val cacheManagerName = new URI(name)
    val cacheManager     = cachingProvider.getCacheManager(cacheManagerName, null, properties)
    cacheManager
  }

  def setSSLOptions(config: HazelCastConnectionConfig) = {
    config.keyStoreLocation match {
      case Some(path) =>
        if (!new File(path).exists) {
          throw new FileNotFoundException(s"Keystore not found in: $path")
        }

        System.setProperty("javax.net.ssl.keyStorePassword", config.keyStorePassword.getOrElse(""))
        System.setProperty("javax.net.ssl.keyStore", path)
        System.setProperty("javax.net.ssl.keyStoreType", config.keyStoreType.getOrElse("jks"))

      case None =>
    }

    config.trustStoreLocation match {
      case Some(path) =>
        if (!new File(path).exists) {
          throw new FileNotFoundException(s"Truststore not found in: $path")
        }

        System.setProperty("javax.net.ssl.trustStorePassword", config.trustStorePassword.getOrElse(""))
        System.setProperty("javax.net.ssl.trustStore", path)
        System.setProperty("javax.net.ssl.trustStoreType", config.trustStoreType.getOrElse("jks"))

      case None =>
    }
  }
}
