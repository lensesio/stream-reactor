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
case class CoapConfig() {

  val config: ConfigDef = new ConfigDef()
    .define(CoapConstants.COAP_KCQL, Type.STRING, Importance.HIGH, CoapConstants.COAP_KCQL_DOC,
      "kcql", 1, ConfigDef.Width.MEDIUM, CoapConstants.COAP_KCQL)
    .define(CoapConstants.COAP_URI, Type.STRING, CoapConstants.COAP_URI_DEFAULT, Importance.HIGH, CoapConstants.COAP_URI_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, CoapConstants.COAP_URI)
    .define(CoapConstants.COAP_TRUST_STORE_PATH, Type.STRING, CoapConstants.COAP_TRUST_STORE_PATH_DEFAULT, Importance.LOW,
      CoapConstants.COAP_TRUST_STORE_PATH_DOC, "Connection", 2, ConfigDef.Width.LONG, CoapConstants.COAP_TRUST_STORE_PATH)
    .define(CoapConstants.COAP_TRUST_STORE_PASS, Type.PASSWORD, CoapConstants.COAP_TRUST_STORE_PASS_DEFAULT, Importance.LOW,
      CoapConstants.COAP_TRUST_STORE_PASS_DOC, "Connection", 3, ConfigDef.Width.LONG, CoapConstants.COAP_TRUST_STORE_PASS)
    .define(CoapConstants.COAP_TRUST_CERTS, Type.LIST, List.empty.asJava, Importance.LOW, CoapConstants.COAP_TRUST_STORE_PASS_DOC,
      "Connection", 4, ConfigDef.Width.LONG, CoapConstants.COAP_TRUST_CERTS)
    .define(CoapConstants.COAP_KEY_STORE_PATH, Type.STRING, CoapConstants.COAP_KEY_STORE_PATH_DEFAULT, Importance.LOW,
      CoapConstants.COAP_KEY_STORE_PATH_DOC, "Connection", 5, ConfigDef.Width.LONG, CoapConstants.COAP_KEY_STORE_PATH)
    .define(CoapConstants.COAP_KEY_STORE_PASS, Type.PASSWORD, CoapConstants.COAP_KEY_STORE_PASS_DEFAULT, Importance.LOW,
      CoapConstants.COAP_KEY_STORE_PASS_DOC, "Connection", 6, ConfigDef.Width.LONG, CoapConstants.COAP_KEY_STORE_PASS)
    .define(CoapConstants.COAP_CERT_CHAIN_KEY, Type.STRING, CoapConstants.COAP_CERT_CHAIN_KEY_DEFAULT, Importance.LOW,
      CoapConstants.COAP_CERT_CHAIN_KEY_DOC, "Connection", 7, ConfigDef.Width.LONG, CoapConstants.COAP_CERT_CHAIN_KEY)

}

object CoapSinkConfig {
  val base = CoapConfig().config

  val config = base
    .define(CoapConstants.COAP_SINK_DTLS_BIND_PORT, Type.INT, CoapConstants.COAP_SINK_DTLS_BIND_PORT_DEFAULT, Importance.LOW, CoapConstants.COAP_SINK_DTLS_BIND_PORT_DOC,
      "Connection", 8,
      ConfigDef.Width.LONG, CoapConstants.COAP_SINK_DTLS_BIND_PORT)
    .define(CoapConstants.COAP_SINK_DTLS_BIND_HOST, Type.STRING, CoapConstants.COAP_SINK_DTLS_BIND_HOST_DEFAULT, Importance.LOW, CoapConstants.COAP_SINK_DTLS_BIND_HOST_DOC,
      "Connection", 9, ConfigDef.Width.LONG, CoapConstants.COAP_SINK_DTLS_BIND_HOST)
    .define(CoapConstants.ERROR_POLICY, Type.STRING, CoapConstants.ERROR_POLICY_DEFAULT, Importance.HIGH, CoapConstants.ERROR_POLICY_DOC,
      "Error", 1, ConfigDef.Width.LONG, CoapConstants.ERROR_POLICY)
    .define(CoapConstants.NBR_OF_RETRIES, Type.INT, CoapConstants.NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, CoapConstants.NBR_OF_RETRIES_DOC,
      "Error", 2, ConfigDef.Width.LONG, CoapConstants.NBR_OF_RETRIES)
    .define(CoapConstants.ERROR_RETRY_INTERVAL, Type.INT, CoapConstants.ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, CoapConstants.ERROR_RETRY_INTERVAL_DOC,
      "Error", 3, ConfigDef.Width.LONG, CoapConstants.ERROR_RETRY_INTERVAL)
}

case class CoapSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(CoapSinkConfig.config, props)

object CoapSourceConfig {
  val base = CoapConfig().config

  val config = base
    .define(CoapConstants.COAP_SOURCE_DTLS_BIND_PORT, Type.INT, CoapConstants.COAP_SOURCE_DTLS_BIND_PORT_DEFAULT, Importance.LOW, CoapConstants.COAP_SINK_DTLS_BIND_PORT_DOC,
      "Connection", 8, ConfigDef.Width.LONG, CoapConstants.COAP_SOURCE_DTLS_BIND_PORT)
    .define(CoapConstants.COAP_SOURCE_DTLS_BIND_HOST, Type.STRING, CoapConstants.COAP_SOURCE_DTLS_BIND_HOST_DEFAULT, Importance.LOW, CoapConstants.COAP_SINK_DTLS_BIND_HOST_DOC,
      "Connection", 9, ConfigDef.Width.LONG, CoapConstants.COAP_SOURCE_DTLS_BIND_HOST)
}

case class CoapSourceConfig(props: util.Map[String, String])
  extends AbstractConfig(CoapSourceConfig.config, props)