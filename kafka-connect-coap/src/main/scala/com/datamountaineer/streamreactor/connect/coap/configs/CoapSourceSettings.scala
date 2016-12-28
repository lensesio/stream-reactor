package com.datamountaineer.streamreactor.connect.coap.configs

import java.io.{File, FileInputStream}
import java.security.cert.Certificate
import java.security.{KeyStore, PrivateKey}

import com.datamountaineer.connector.config.Config
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConverters._

case class Store(privateKey: PrivateKey, store: KeyStore, certs : Array[Certificate])


/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
case class CoapSourceSetting(uri: String, keyStore: Option[Store], trustStore: Option[Store], chainKey: String, kcql : Config)
case class CoapSourceSettings(settings: Set[CoapSourceSetting])


object CoapSourceSettings {
  def apply(config: CoapSourceConfig): CoapSourceSettings = {
    val uri = config.getString(CoapSourceConfig.COAP_URI)
    val keyStoreLoc = config.getString(CoapSourceConfig.COAP_KEY_STORE_PATH)
    val keyStorePass = config.getPassword(CoapSourceConfig.COAP_KEY_STORE_PASS)
    val trustStoreLoc = config.getString(CoapSourceConfig.COAP_TRUST_STORE_PATH)
    val trustStorePass = config.getPassword(CoapSourceConfig.COAP_TRUST_STORE_PASS)
    val certs = config.getList(CoapSourceConfig.COAP_TRUST_CERTS).asScala.toArray
    val chainKey = config.getString(CoapSourceConfig.COAP_KEY_STORE_CHAIN_KEY)

    val keyStore = keyStoreLoc match {
      case loc if (loc.nonEmpty && !new File(loc).exists()) => {
        throw new ConfigException(s"${CoapSourceConfig.COAP_KEY_STORE_PATH} is invalid. Can't locate $loc")
      }
      case loc if (loc.nonEmpty) => Some(getStore(true, loc, keyStorePass.value(), chainKey, Array.empty))
      case loc if (loc.isEmpty) => None
    }

    val trustStore = trustStoreLoc match {
      case loc if (loc.nonEmpty && !new File(loc).exists()) => {
        throw new ConfigException(s"${CoapSourceConfig.COAP_TRUST_STORE_PATH} is invalid. Can't locate $loc")
      }
      case loc if (loc.nonEmpty) => Some(getStore(false, loc, trustStorePass.value(), chainKey, certs))
      case loc if (loc.isEmpty) => None
    }

    val raw = config.getString(CoapSourceConfig.COAP_KCQL)
    require(raw != null && !raw.isEmpty,  s"No ${CoapSourceConfig.COAP_KCQL} provided!")
    val routes = raw.split(";").map(r => Config.parse(r)).toSet

    CoapSourceSettings(routes.map(r => new CoapSourceSetting(uri, keyStore, trustStore, chainKey, r)))
  }

  /**
    * Get a key/trust store
    *
    * @param keyStore Is key store
    * @param location Path to the store
    * @param password Password for the store
    * @return A KeyStore
    * */
  def getStore(keyStore: Boolean, location: String, password: String, chainKey: String, certs: Array[String]): Store = {
    val store = KeyStore.getInstance("JKS")
    val in = new FileInputStream(location)
    store.load(in, password.toCharArray())
    in.close()

    val privateKey = store.getKey(chainKey, password.toCharArray)
    if (keyStore  && privateKey == null) {
      throw new ConfigException(s"${CoapSourceConfig.COAP_KEY_STORE_CHAIN_KEY} $chainKey not found! in ${location}")
    }

    val certificates = certs.map(c => store.getCertificate(c))
    Store(privateKey.asInstanceOf[PrivateKey], store, certificates)
  }
}