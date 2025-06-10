/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.azure.cosmosdb

import cats.implicits._
import com.azure.core.http.ProxyOptions
import com.azure.cosmos.CosmosClient
import com.azure.cosmos.CosmosClientBuilder
import com.azure.cosmos.GatewayConnectionConfig
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import org.apache.kafka.connect.errors.ConnectException

import java.net.InetSocketAddress
import scala.util.Try

/**
 * Creates an instance of Azure DocumentClient class
 */
object CosmosClientProvider {
  def get(settings: CosmosDbSinkSettings): Either[ConnectException, CosmosClient] = Try {

    val gateway = settings.proxy.map { proxy =>
      val proxyOptions = new ProxyOptions(
        ProxyOptions.Type.HTTP,
        new InetSocketAddress(proxy, 8080),
      )

      new GatewayConnectionConfig()
        .setProxy(proxyOptions)
    }.getOrElse(GatewayConnectionConfig.getDefaultConfig)

    new CosmosClientBuilder()
      .endpoint(settings.endpoint)
      .key(settings.masterKey)
      .gatewayMode(gateway)
      .consistencyLevel(settings.consistency)
      .buildClient()

  }.toEither.leftMap {
    case npe: NullPointerException =>
      new ConnectException("Null value found in CosmosClient settings, please check your configuration.", npe)
    case ex: IllegalArgumentException =>
      new ConnectException(s"Exception while creating CosmosClient, ${ex.getMessage}", ex)

  }
}
