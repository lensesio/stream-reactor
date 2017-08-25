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

import java.io.FileInputStream
import java.net.{InetAddress, InetSocketAddress}
import java.security.cert.Certificate
import java.security.{KeyStore, PrivateKey}

import com.datamountaineer.streamreactor.connect.coap.configs.{CoapConstants, CoapSetting}
import org.apache.kafka.common.config.ConfigException
import org.eclipse.californium.scandium.config.DtlsConnectorConfig
import org.eclipse.californium.scandium.dtls.cipher.CipherSuite
import org.eclipse.californium.scandium.dtls.pskstore.InMemoryPskStore

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
object DTLSConnectionFn {
  def apply(setting: CoapSetting): Either[DtlsConnectorConfig, Unit] = {
    val addr = new InetSocketAddress(InetAddress.getByName(setting.bindHost), setting.bindPort)
    val builder = new DtlsConnectorConfig.Builder
    builder.setAddress(addr)

    if (setting.keyStoreLoc != null && setting.keyStoreLoc.nonEmpty) {
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

      builder.setIdentity(privateKey, certChain, true)
      builder.setTrustStore(certificates)
      Left(builder.build())

    } else if (setting.identity.nonEmpty) {

      if (!setting.privateKey.isDefined) {
        throw new ConfigException(s"${CoapConstants.COAP_PRIVATE_KEY_FILE} not defined")
      }

      if (!setting.publicKey.isDefined) {
        throw new ConfigException(s"${CoapConstants.COAP_PUBLIC_KEY_FILE} not defined")
      }

      val psk = new InMemoryPskStore()
      builder.setSupportedCipherSuites(Array[CipherSuite](CipherSuite.TLS_PSK_WITH_AES_128_CCM_8, CipherSuite.TLS_PSK_WITH_AES_128_CBC_SHA256))
      psk.setKey(setting.identity, setting.secret.value().getBytes())
      psk.addKnownPeer(addr, setting.identity, setting.secret.value().getBytes())
      builder.setPskStore(psk)
      builder.setIdentity(setting.privateKey.get, setting.publicKey.get)

      Left(builder.build())

    } else {
      Right(())
    }
  }
}
