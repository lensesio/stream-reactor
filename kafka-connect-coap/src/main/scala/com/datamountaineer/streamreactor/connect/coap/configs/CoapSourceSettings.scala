package com.datamountaineer.streamreactor.connect.coap.configs

import java.io.File
import java.security.cert.Certificate
import java.security.{KeyStore, PrivateKey}

import com.datamountaineer.connector.config.Config
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password

import scala.collection.JavaConverters._

case class Store(privateKey: PrivateKey, store: KeyStore, certs : Array[Certificate])


/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
case class CoapSourceSetting(uri: String,
                             keyStoreLoc: String,
                             keyStorePass: Password,
                             trustStoreLoc: String,
                             trustStorePass: Password,
                             certs: Array[String],
                             chainKey: String,
                             kcql : Config)

object CoapSourceSettings {
  def apply(config: CoapSourceConfig): Set[CoapSourceSetting] = {
    val uri = config.getString(CoapSourceConfig.COAP_URI)
    val keyStoreLoc = config.getString(CoapSourceConfig.COAP_KEY_STORE_PATH)
    val keyStorePass = config.getPassword(CoapSourceConfig.COAP_KEY_STORE_PASS)
    val trustStoreLoc = config.getString(CoapSourceConfig.COAP_TRUST_STORE_PATH)
    val trustStorePass = config.getPassword(CoapSourceConfig.COAP_TRUST_STORE_PASS)
    val certs = config.getList(CoapSourceConfig.COAP_TRUST_CERTS).asScala.toArray
    val certChainKey = config.getString(CoapSourceConfig.COAP_CERT_CHAIN_KEY)

    if (keyStoreLoc.nonEmpty && !new File(keyStoreLoc).exists()) {
      throw new ConfigException(s"${CoapSourceConfig.COAP_KEY_STORE_PATH} is invalid. Can't locate $keyStoreLoc")
    }

    if (trustStoreLoc.nonEmpty && !new File(trustStoreLoc).exists()) {
      throw new ConfigException(s"${CoapSourceConfig.COAP_TRUST_STORE_PATH} is invalid. Can't locate $trustStoreLoc")
    }

    val raw = config.getString(CoapSourceConfig.COAP_KCQL)
    require(raw != null && !raw.isEmpty,  s"No ${CoapSourceConfig.COAP_KCQL} provided!")
    val routes = raw.split(";").map(r => Config.parse(r)).toSet

    routes.map(r => new CoapSourceSetting(uri, keyStoreLoc, keyStorePass, trustStoreLoc, trustStorePass, certs, certChainKey, r))
  }
}