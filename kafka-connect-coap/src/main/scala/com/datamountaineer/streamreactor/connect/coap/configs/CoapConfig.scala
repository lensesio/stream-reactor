/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */

package com.datamountaineer.streamreactor.connect.coap.configs

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
object CoapConfig {

  val COAP_KCQL = "connect.coap.source.kcql"
  val COAP_KCQL_DOC = "The KCQL statement to select and route resources to topics."

  val COAP_PAYLOAD_CONVERTERS = "connect.coap.source.payload.converters"
  val COAP_PAYLOAD_CONVERTERS_DOC = "The coap message payload converters to converter the payload to a Connect Struct."
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

  val COAP_CERT_CHAIN_KEY = "connect.coap.source.cert.chain.key"
  val COAP_CERT_CHAIN_KEY_DOC = "The key to use to get the certificate chain."
  val COAP_CERT_CHAIN_KEY_DEFAULT = "client"

  val ERROR_POLICY = "connect.coap.error.policy"
  val ERROR_POLICY_DOC: String =
    """
      |Specifies the action to be taken if an error occurs while inserting the data.
      | There are three available options:
      |    NOOP - the error is swallowed
      |    THROW - the error is allowed to propagate.
      |    RETRY - The exception causes the Connect framework to retry the message. The number of retries is set by connect.cassandra.max.retires.
      |All errors will be logged automatically, even if the code swallows them.
    """.stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = "connect.coap.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES = "connect.coap.max.retires"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val COAP_DISCOVER_IP4 = "discoverIP4"
  val COAP_DISCOVER_IP6 = "discoverIP6"
  val COAP_DISCOVER_IP4_ADDRESS = "224.0.1.187"
  val COAP_DISCOVER_IP6_ADDRESS = "FF05::FD"

  val config: ConfigDef = new ConfigDef()
    .define(COAP_KCQL, Type.STRING, Importance.HIGH, COAP_KCQL_DOC,
      "kcql", 1, ConfigDef.Width.MEDIUM, COAP_KCQL)
    .define(COAP_URI, Type.STRING, COAP_URI_DEFAULT, Importance.HIGH, COAP_URI_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, COAP_URI)
    .define(COAP_TRUST_STORE_PATH, Type.STRING, COAP_TRUST_STORE_PATH_DEFAULT, Importance.LOW, COAP_TRUST_STORE_PATH_DOC,
      "Connection", 2, ConfigDef.Width.LONG, COAP_TRUST_STORE_PATH)
    .define(COAP_TRUST_STORE_PASS, Type.PASSWORD, COAP_TRUST_STORE_PASS_DEFAULT, Importance.LOW, COAP_TRUST_STORE_PASS_DOC,
      "Connection", 3, ConfigDef.Width.LONG, COAP_TRUST_STORE_PASS)
    .define(COAP_TRUST_CERTS, Type.LIST, List.empty.asJava, Importance.LOW, COAP_TRUST_STORE_PASS_DOC,
      "Connection", 4, ConfigDef.Width.LONG, COAP_TRUST_CERTS)
    .define(COAP_KEY_STORE_PATH, Type.STRING, COAP_KEY_STORE_PATH_DEFAULT, Importance.LOW, COAP_KEY_STORE_PATH_DOC,
      "Connection", 5, ConfigDef.Width.LONG, COAP_KEY_STORE_PATH)
    .define(COAP_KEY_STORE_PASS, Type.PASSWORD, COAP_KEY_STORE_PASS_DEFAULT, Importance.LOW, COAP_KEY_STORE_PASS_DOC,
      "Connection", 6, ConfigDef.Width.LONG, COAP_KEY_STORE_PASS)
    .define(COAP_CERT_CHAIN_KEY, Type.STRING, COAP_CERT_CHAIN_KEY_DEFAULT, Importance.LOW, COAP_CERT_CHAIN_KEY_DOC,
      "Connection", 7, ConfigDef.Width.LONG, COAP_CERT_CHAIN_KEY)
    .define(ERROR_POLICY, Type.STRING, ERROR_POLICY_DEFAULT, Importance.HIGH, ERROR_POLICY_DOC,
      "Error", 1, ConfigDef.Width.LONG, ERROR_POLICY)
    .define(NBR_OF_RETRIES, Type.INT, NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, NBR_OF_RETRIES_DOC,
      "Error", 2, ConfigDef.Width.LONG, NBR_OF_RETRIES)
    .define(ERROR_RETRY_INTERVAL, Type.INT, ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, ERROR_RETRY_INTERVAL_DOC,
      "Error", 3, ConfigDef.Width.LONG, ERROR_RETRY_INTERVAL)

}

case class CoapConfig(props: util.Map[String, String])
  extends AbstractConfig(CoapConfig.config, props)


