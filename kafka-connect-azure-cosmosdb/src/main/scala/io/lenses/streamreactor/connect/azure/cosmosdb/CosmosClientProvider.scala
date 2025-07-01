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
import java.net.MalformedURLException
import java.net.URI
import scala.util.Try

/**
 * Creates an instance of Azure DocumentClient class
 */
object CosmosClientProvider {
  def get(settings: CosmosDbSinkSettings): Either[ConnectException, CosmosClient] =
    for {
      gateway <- configureGateway(settings)
      client  <- createCosmosClient(settings, gateway)
    } yield client

  private def configureGateway(settings: CosmosDbSinkSettings): Either[ConnectException, GatewayConnectionConfig] = {
    val gatewayConfig = GatewayConnectionConfig.getDefaultConfig
    settings.proxy
      .map(convertProxy)
      .fold(gatewayConfig.asRight[ConnectException]) {
        case Left(mue)    => new ConnectException(s"Proxy configuration incorrect, ${mue.getMessage}", mue).asLeft
        case Right(proxy) => gatewayConfig.setProxy(proxy).asRight
      }
  }

  private def createCosmosClient(
    settings: CosmosDbSinkSettings,
    gateway:  GatewayConnectionConfig,
  ): Either[ConnectException, CosmosClient] =
    Try(
      new CosmosClientBuilder()
        .endpoint(settings.endpoint)
        .key(settings.masterKey)
        .gatewayMode(gateway)
        .consistencyLevel(settings.consistency)
        .buildClient(),
    )
      .toEither
      .leftMap {
        case npe: NullPointerException =>
          new ConnectException("Null value found in CosmosClient settings, please check your configuration.", npe)
        case ex: IllegalArgumentException =>
          new ConnectException(s"Exception while creating CosmosClient, ${ex.getMessage}", ex)
      }

  private[cosmosdb] def convertProxy(proxy: String): Either[MalformedURLException, ProxyOptions] =
    Try(new URI(proxy)).toEither.leftMap(_ => new MalformedURLException("Invalid proxy URI")).flatMap { url =>
      val protocolOpt = Option(url.getScheme).map(_.toLowerCase)
      val hostOpt     = Option(url.getHost)
      val portOpt     = Option(url.getPort).filter(_ != -1)

      for {
        protocol <- protocolOpt.toRight(new MalformedURLException("Proxy protocol has not been specified"))
        host     <- hostOpt.toRight(new MalformedURLException("Proxy host has not been specified"))
        port     <- portOpt.toRight(new MalformedURLException("Proxy port has not been specified"))
        proxyType <- protocol match {
          case "http"   => Right(ProxyOptions.Type.HTTP)
          case "socks4" => Right(ProxyOptions.Type.SOCKS4)
          case "socks5" => Right(ProxyOptions.Type.SOCKS5)
          case _        => Left(new MalformedURLException("Unsupported proxy protocol specified"))
        }
      } yield new ProxyOptions(proxyType, new InetSocketAddress(host, port))
    }

}
