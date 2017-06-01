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

package com.datamountaineer.streamreactor.connect.coap.connection

import java.net.{ConnectException, URI}

import com.datamountaineer.streamreactor.connect.coap.configs.{CoapConstants, CoapSetting}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.eclipse.californium.core.network.CoapEndpoint
import org.eclipse.californium.core.network.config.NetworkConfig
import org.eclipse.californium.core.{CoapClient, CoapResponse}
import org.eclipse.californium.scandium.DTLSConnector

/**
  * Created by andrew@datamountaineer.com on 29/12/2016. 
  * stream-reactor
  */
abstract class CoapManager(setting: CoapSetting) extends StrictLogging {

  val configUri = new URI(setting.uri)

  val uri: URI = configUri.getHost match {
    case CoapConstants.COAP_DISCOVER_IP4 => discoverServer(CoapConstants.COAP_DISCOVER_IP4_ADDRESS, configUri)
    case CoapConstants.COAP_DISCOVER_IP6 => discoverServer(CoapConstants.COAP_DISCOVER_IP6_ADDRESS, configUri)
    case _ => configUri
  }

  val client: CoapClient = buildClient(uri)

  def buildClient(uri: URI): CoapClient = {
    val client = new CoapClient(uri)
    //Use DTLS is key stores defined
    if (setting.keyStoreLoc != null && setting.keyStoreLoc.nonEmpty) {
      logger.info("Creating secure client")
      client.setEndpoint(new CoapEndpoint(new DTLSConnector(DTLSConnectionFn(setting)), NetworkConfig.getStandard()))
    }

    import scala.collection.JavaConverters._
    //discover and check the requested resources
    Option(client.discover())
      .map(_.asScala)
      .getOrElse(Set.empty).map(r => {
      logger.info(s"Discovered resources ${r.getURI}")
      r.getURI
    })

    client.setURI(s"${setting.uri}/${setting.target}")
    client
  }

  def delete(): CoapResponse = client.delete()

  /**
    * Discover servers on the local network
    * and return the first one
    *
    * @param address The multicast address (ip4 or ip6)
    * @param uri  The original URI
    * @return A new URI of the server
    **/
  def discoverServer(address: String, uri: URI): URI = {
    val client = new CoapClient(s"${uri.getScheme}://$address:${uri.getPort.toString}/.well-known/core")
    client.useNONs()
    val response = client.get()

    if (response != null) {
      logger.info(s"Discovered Server ${response.advanced().getSource.toString}.")
      new URI(uri.getScheme,
        uri.getUserInfo,
        response.advanced().getSource.getHostName,
        response.advanced().getSourcePort,
        uri.getPath,
        uri.getQuery,
        uri.getFragment)
    } else {
      logger.error(s"Unable to find any servers on local network with multicast address $address.")
      throw new ConnectException(s"Unable to find any servers on local network with multicast address $address.")
    }
  }
}
