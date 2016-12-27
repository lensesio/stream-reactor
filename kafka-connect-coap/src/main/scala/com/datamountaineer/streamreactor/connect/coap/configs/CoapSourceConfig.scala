package com.datamountaineer.streamreactor.connect.coap.configs

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
object CoapSourceConfig {

  val COAP_KCQL = "connect.coap.source.kcql"
  val COAP_KCQL_DOC = "The KCQL statement to select and route resources to topics."

  val COAP_PAYLOAD_CONVERTERS = "connect.coap.source.payload.converters"
  val COAP_PAYLOAD_CONVERTERS_DOC = "The coap message payload converters to conveter the payload to a Connect Struct."
  val COAP_PAYLOAD_CONVERTERS_DEFAULT = "string"

  val COAP_URI = "connect.coap.source.uri"
  val COAP_URI_DOC = "The COAP server to connect to."
  val COAP_URI_DEFAULT = "localhost"

  //Security
  val COAP_TRUST_STORE_PASS = "connect.coap.source.truststore.pass"
  val COAP_TRUST_STORE_PASS_DOC = "The password of the trust store."
  val COAP_TRUST_STORE_PASS_DEFAULT = "rootPass"

  val COAP_TRUST_STORE_PATH = "connect.coap.source.truststore.path"
  val COAP_TRUST_STORE_PATH_DOC = "The path to the truststore."
  val COAP_TRUST_STORE_PATH_DEFAULT = ""

  val COAP_TRUST_CERTS = "connect.coap.source.certs"
  val COAP_TRUST_CERTS_DOC = "The certificates to load from the trust store"

  val COAP_KEY_STORE_PASS = "connect.coap.source.keystore.pass"
  val COAP_KEY_STORE_PASS_DOC = "The password of the key store."
  val COAP_KEY_STORE_PASS_DEFAULT = "rootPass"

  val COAP_KEY_STORE_PATH = "connect.coap.source.keystore.path"
  val COAP_KEY_STORE_PATH_DOC = "The path to the truststore."
  val COAP_KEY_STORE_PATH_DEFAULT = ""

  val COAP_KEY_STORE_CHAIN_KEY = "connect.coap.source.chain.key"
  val COAP_KEY_STORE_CHAIN_DOC = "The key to use to get the certificate chain and private key for the keystore."
  val COAP_KEY_STORE_CHAIN_DEFAULT = "client"

  val config = new ConfigDef()
    .define(COAP_KCQL, Type.STRING, Importance.HIGH, COAP_KCQL_DOC,
      "kcql", 1, ConfigDef.Width.MEDIUM, COAP_KCQL)

    .define(COAP_URI, Type.STRING, COAP_URI_DEFAULT, Importance.HIGH, COAP_URI_DOC,
      "Connection", 2, ConfigDef.Width.MEDIUM, COAP_URI)
    .define(COAP_TRUST_STORE_PATH, Type.STRING, COAP_TRUST_STORE_PATH_DEFAULT, Importance.LOW, COAP_TRUST_STORE_PATH_DOC,
      "Connection", 3, ConfigDef.Width.LONG, COAP_TRUST_STORE_PATH)
    .define(COAP_TRUST_STORE_PASS, Type.PASSWORD, COAP_TRUST_STORE_PASS_DEFAULT, Importance.LOW, COAP_TRUST_STORE_PASS_DOC,
      "Connection", 4, ConfigDef.Width.LONG, COAP_TRUST_STORE_PASS)
    .define(COAP_TRUST_CERTS, Type.LIST, List.empty.asJava, Importance.LOW, COAP_TRUST_STORE_PASS_DOC,
      "Connection", 5, ConfigDef.Width.LONG, COAP_TRUST_CERTS)
    .define(COAP_KEY_STORE_PATH, Type.STRING, COAP_KEY_STORE_PATH_DEFAULT, Importance.LOW, COAP_KEY_STORE_PATH_DOC,
      "Connection", 6, ConfigDef.Width.LONG, COAP_KEY_STORE_PATH)
    .define(COAP_KEY_STORE_PASS, Type.PASSWORD, COAP_KEY_STORE_PASS_DEFAULT, Importance.LOW, COAP_KEY_STORE_PASS_DOC,
      "Connection", 7, ConfigDef.Width.LONG, COAP_KEY_STORE_PASS)
    .define(COAP_KEY_STORE_CHAIN_KEY, Type.STRING, COAP_KEY_STORE_CHAIN_DEFAULT, Importance.LOW, COAP_KEY_STORE_CHAIN_DOC,
      "Connection", 8, ConfigDef.Width.LONG, COAP_KEY_STORE_CHAIN_KEY)

}

case class CoapSourceConfig(props: util.Map[String, String])
  extends AbstractConfig(CoapSourceConfig.config, props)


