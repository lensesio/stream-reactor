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

import com.datamountaineer.streamreactor.connect.coap.configs.CoapSetting
import org.eclipse.californium.scandium.config.DtlsConnectorConfig

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
object DTLSConnectionFn {
  def apply(setting: CoapSetting): DtlsConnectorConfig = {
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

    val addr = new InetSocketAddress(InetAddress.getByName(setting.bindHost), setting.bindPort)
    val builder = new DtlsConnectorConfig.Builder(addr)
    //builder.setPskStore(new StaticPskStore("Client_identity", "secretPSK".getBytes()))
    builder.setIdentity(privateKey, certChain, true)
    builder.setTrustStore(certificates)
    builder.build()
  }
}
