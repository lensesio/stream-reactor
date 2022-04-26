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

import com.datamountaineer.streamreactor.connect.coap.configs.{CoapConstants, CoapSetting}
import com.google.common.collect.Lists
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.eclipse.californium.core.CoapClient
import org.eclipse.californium.core.coap.CoAP
import org.eclipse.californium.core.network.CoapEndpoint
import org.eclipse.californium.elements.config.Configuration
import org.eclipse.californium.scandium.DTLSConnector
import org.eclipse.californium.scandium.config.{DtlsConfig, DtlsConnectorConfig}
import org.eclipse.californium.scandium.dtls.CertificateType
import org.eclipse.californium.scandium.dtls.cipher.CipherSuite
import org.eclipse.californium.scandium.dtls.pskstore.AdvancedMultiPskStore
import org.eclipse.californium.scandium.dtls.x509.{SingleCertificateProvider, StaticNewAdvancedCertificateVerifier}

import java.io.FileInputStream
import java.net.{InetAddress, InetSocketAddress, URI}
import java.security.cert.Certificate
import java.security.{KeyStore, PrivateKey}

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
object DTLSConnectionFn extends StrictLogging {
  def apply(setting: CoapSetting): CoapClient = {

    val configUri = new URI(setting.uri)

    val uri: URI = configUri.getHost match {
      case CoapConstants.COAP_DISCOVER_IP4 => discoverServer(CoapConstants.COAP_DISCOVER_IP4_ADDRESS, configUri)
      case CoapConstants.COAP_DISCOVER_IP6 => discoverServer(CoapConstants.COAP_DISCOVER_IP6_ADDRESS, configUri)
      case _ => configUri
    }

    val client: CoapClient = new CoapClient(uri)
    val addr = new InetSocketAddress(InetAddress.getByName(setting.bindHost), setting.bindPort)
    val builder = DtlsConnectorConfig.builder(Configuration.getStandard).setAddress(addr)

    if (uri.getScheme.equals(CoAP.COAP_SECURE_URI_SCHEME)) {

      //Use SSL
      if (setting.identity.isEmpty) {
        val keyStore = KeyStore.getInstance("JKS")
        val inKey = new FileInputStream(setting.keyStoreLoc)
        keyStore.load(inKey, setting.keyStorePass.value().toCharArray())
        inKey.close()

        val trustStore = KeyStore.getInstance("JKS")
        val inTrust = new FileInputStream(setting.trustStoreLoc)
        trustStore.load(inTrust, setting.trustStorePass.value().toCharArray())
        inTrust.close()

        val certificates: Array[Certificate] = setting.certs.map(c => trustStore.getCertificate(c))
        val privateKey = keyStore.getKey(setting.chainKey, setting.keyStorePass.value().toCharArray).asInstanceOf[PrivateKey]
        val certChain = keyStore.getCertificateChain(setting.chainKey)

        builder.setCertificateIdentityProvider(
          new SingleCertificateProvider(privateKey, certChain, CertificateType.RAW_PUBLIC_KEY, CertificateType.X_509)
        )
        builder.setAdvancedCertificateVerifier(StaticNewAdvancedCertificateVerifier
          .builder()
          .setTrustedCertificates(certificates: _*)
          .build())
      } else {

        val psk = new AdvancedMultiPskStore()
        psk.setKey(setting.identity, setting.secret.value().getBytes())
        psk.addKnownPeer(addr, setting.identity, setting.secret.value().getBytes())
        builder.setAdvancedPskStore(psk)

        if (setting.privateKey.isDefined) {
          builder.set(DtlsConfig.DTLS_CIPHER_SUITES, Lists.newArrayList(CipherSuite.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, CipherSuite.TLS_PSK_WITH_AES_128_CCM_8, CipherSuite.TLS_PSK_WITH_AES_128_CBC_SHA256))
          builder.setCertificateIdentityProvider(
            new SingleCertificateProvider(
              setting.privateKey.get,
              setting.publicKey.get
            )
          )
          import org.eclipse.californium.scandium.dtls.x509.StaticNewAdvancedCertificateVerifier
          val verifier = StaticNewAdvancedCertificateVerifier.builder.setTrustAllRPKs.build
          builder.setAdvancedCertificateVerifier(verifier)
          //builder.setAdvancedCertificateVerifier(StaticNewAdvancedCertificateVerifier.builder()
          //  .setTrustedRPKs(new RawPublicKeyIdentity(setting.publicKey.get)).build());
        }

      }

      client.setEndpoint(new CoapEndpoint.Builder()
        .setConnector(new DTLSConnector(builder.build()))
        .build())
    }
    client.setURI(s"${setting.uri}/${setting.target}")
  }

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
      logger.info(s"Discovered Server ${response.advanced().getSourceContext.toString}.")
      new URI(uri.getScheme,
        uri.getUserInfo,
        response.advanced().getSourceContext.getPeerAddress.getHostName,
        response.advanced().getSourceContext.getPeerAddress.getPort,
        uri.getPath,
        uri.getQuery,
        uri.getFragment)
    } else {
      logger.error(s"Unable to find any servers on local network with multicast address $address.")
      throw new ConnectException(s"Unable to find any servers on local network with multicast address $address.")
    }
  }
}
