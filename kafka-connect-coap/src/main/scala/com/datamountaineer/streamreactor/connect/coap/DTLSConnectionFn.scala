package com.datamountaineer.streamreactor.connect.coap

import java.net.InetSocketAddress

import com.datamountaineer.streamreactor.connect.coap.configs.{CoapSourceConfig, CoapSourceSetting}
import org.eclipse.californium.scandium.DTLSConnector
import org.eclipse.californium.scandium.config.DtlsConnectorConfig

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
object DTLSConnectionFn {
  def apply(setting: CoapSourceSetting) = {
    val address = new InetSocketAddress(0)
    val builder = new DtlsConnectorConfig.Builder(address)
    builder.setClientOnly()
    //builder.setPskStore(new StaticPskStore("Client_identity", "secretPSK".getBytes()))
    builder.setIdentity(setting.keyStore.get.privateKey,
                        setting.keyStore.get.store.getCertificateChain(CoapSourceConfig.COAP_KEY_STORE_CHAIN_KEY),
                        true)
    builder.setTrustStore(setting.trustStore.get.certs)
    new DTLSConnector(builder.build())
  }
}
