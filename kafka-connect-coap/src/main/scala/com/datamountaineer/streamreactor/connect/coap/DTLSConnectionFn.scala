package com.datamountaineer.streamreactor.connect.coap

import java.net.InetSocketAddress

import com.datamountaineer.streamreactor.connect.coap.configs.CoapSourceSetting
import org.eclipse.californium.scandium.DTLSConnector
import org.eclipse.californium.scandium.config.DtlsConnectorConfig
import java.net.URI

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
object DTLSConnectionFn {
  def apply(setting: CoapSourceSetting) = {
    val uri = new URI(setting.uri)
    val address = new InetSocketAddress(uri.getHost, uri.getPort)
    val builder = new DtlsConnectorConfig.Builder(address)
    builder.setClientOnly()
    //builder.setPskStore(new StaticPskStore("Client_identity", "secretPSK".getBytes()))
    builder.setIdentity(setting.keyStore.get.privateKey,
                        setting.keyStore.get.store.getCertificateChain(setting.chainKey),
                        true)
    builder.setTrustStore(setting.trustStore.get.certs)
    new DTLSConnector(builder.build())
  }
}
