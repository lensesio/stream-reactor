package com.datamountaineer.streamreactor.connect.coap

import java.io.FileInputStream
import java.net.InetSocketAddress

import com.datamountaineer.streamreactor.connect.coap.configs.CoapSourceSetting
import org.eclipse.californium.scandium.DTLSConnector
import org.eclipse.californium.scandium.config.DtlsConnectorConfig
import java.net.URI
import java.security.cert.Certificate
import java.security.{KeyStore, PrivateKey}

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
object DTLSConnectionFn {
  def apply(setting: CoapSourceSetting) = {
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

    val builder = new DtlsConnectorConfig.Builder(new InetSocketAddress(0))
    //builder.setPskStore(new StaticPskStore("Client_identity", "secretPSK".getBytes()))
    builder.setIdentity(privateKey, certChain, true)
    builder.setTrustStore(certificates)
    builder.build()
  }
}
