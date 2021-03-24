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

package com.datamountaineer.streamreactor.connect.coap.configs

import com.datamountaineer.streamreactor.common.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

import java.util
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
object CoapConfig {

  val config: ConfigDef = new ConfigDef()
    .define(CoapConstants.COAP_KCQL, Type.STRING, Importance.HIGH, CoapConstants.COAP_KCQL_DOC,
      "kcql", 1, ConfigDef.Width.MEDIUM, CoapConstants.COAP_KCQL)

    .define(CoapConstants.COAP_URI, Type.STRING, CoapConstants.COAP_URI_DEFAULT, Importance.HIGH, CoapConstants.COAP_URI_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, CoapConstants.COAP_URI)

    .define(CoapConstants.COAP_TRUST_STORE_PATH, Type.STRING, CoapConstants.COAP_TRUST_STORE_PATH_DEFAULT, Importance.LOW, CoapConstants.COAP_TRUST_STORE_PATH_DOC,
      "Connection", 2, ConfigDef.Width.LONG, CoapConstants.COAP_TRUST_STORE_PATH)

    .define(CoapConstants.COAP_TRUST_STORE_PASS, Type.PASSWORD, CoapConstants.COAP_TRUST_STORE_PASS_DEFAULT, Importance.LOW, CoapConstants.COAP_TRUST_STORE_PASS_DOC,
      "Connection", 3, ConfigDef.Width.LONG, CoapConstants.COAP_TRUST_STORE_PASS)

    .define(CoapConstants.COAP_TRUST_CERTS, Type.LIST, List.empty.asJava, Importance.LOW, CoapConstants.COAP_TRUST_STORE_PASS_DOC,
      "Connection", 4, ConfigDef.Width.LONG, CoapConstants.COAP_TRUST_CERTS)

    .define(CoapConstants.COAP_KEY_STORE_PATH, Type.STRING, CoapConstants.COAP_KEY_STORE_PATH_DEFAULT, Importance.LOW, CoapConstants.COAP_KEY_STORE_PATH_DOC,
      "Connection", 5, ConfigDef.Width.LONG, CoapConstants.COAP_KEY_STORE_PATH)

    .define(CoapConstants.COAP_KEY_STORE_PASS, Type.PASSWORD, CoapConstants.COAP_KEY_STORE_PASS_DEFAULT, Importance.LOW, CoapConstants.COAP_KEY_STORE_PASS_DOC,
      "Connection", 6, ConfigDef.Width.LONG, CoapConstants.COAP_KEY_STORE_PASS)

    .define(CoapConstants.COAP_CERT_CHAIN_KEY, Type.STRING, CoapConstants.COAP_CERT_CHAIN_KEY_DEFAULT, Importance.LOW, CoapConstants.COAP_CERT_CHAIN_KEY_DOC,
      "Connection", 7, ConfigDef.Width.LONG, CoapConstants.COAP_CERT_CHAIN_KEY)

    .define(CoapConstants.COAP_DTLS_BIND_PORT, Type.INT, CoapConstants.COAP_DTLS_BIND_PORT_DEFAULT, Importance.LOW, CoapConstants.COAP_DTLS_BIND_PORT_DOC,
      "Connection", 8,
      ConfigDef.Width.LONG, CoapConstants.COAP_DTLS_BIND_PORT)

    .define(CoapConstants.COAP_DTLS_BIND_HOST, Type.STRING, CoapConstants.COAP_DTLS_BIND_HOST_DEFAULT, Importance.LOW, CoapConstants.COAP_DTLS_BIND_HOST_DOC,
      "Connection", 9, ConfigDef.Width.LONG, CoapConstants.COAP_DTLS_BIND_HOST)

    .define(CoapConstants.COAP_IDENTITY, Type.STRING, CoapConstants.COAP_IDENTITY_DEFAULT, Importance.LOW, CoapConstants.COAP_IDENTITY_DOC,
      "Connection", 10, ConfigDef.Width.LONG, CoapConstants.COAP_IDENTITY)

    .define(CoapConstants.COAP_SECRET, Type.PASSWORD, CoapConstants.COAP_SECRET_DEFAULT, Importance.LOW, CoapConstants.COAP_SECRET_DOC,
      "Connection", 11, ConfigDef.Width.LONG, CoapConstants.COAP_SECRET)

    .define(CoapConstants.COAP_PRIVATE_KEY_FILE, Type.STRING, CoapConstants.COAP_PRIVATE_KEY_FILE_DEFAULT, Importance.LOW, CoapConstants.COAP_PRIVATE_KEY_FILE_DOC,
      "Connection", 12, ConfigDef.Width.LONG, CoapConstants.COAP_PRIVATE_KEY_FILE)

    .define(CoapConstants.COAP_PUBLIC_KEY_FILE, Type.STRING, CoapConstants.COAP_PUBLIC_KEY_FILE_DEFAULT, Importance.LOW, CoapConstants.COAP_PUBLIC_KEY_FILE_DOC,
      "Connection", 12, ConfigDef.Width.LONG, CoapConstants.COAP_PUBLIC_KEY_FILE)

    .define(CoapConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, CoapConstants.PROGRESS_COUNTER_ENABLED_DEFAULT, Importance.MEDIUM, CoapConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, CoapConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)

    .define(CoapConstants.BATCH_SIZE, Type.INT, CoapConstants.BATCH_SIZE_DEFAULT, Importance.MEDIUM, CoapConstants.BATCH_SIZE_DOC,
      "Metrics", 2, ConfigDef.Width.MEDIUM, CoapConstants.BATCH_SIZE)

    .define(CoapConstants.SOURCE_LINGER_MS, Type.INT, CoapConstants.SOURCE_LINGER_MS_DEFAULT, Importance.MEDIUM, CoapConstants.SOURCE_LINGER_MS_DOC,
      "Metrics", 3, ConfigDef.Width.MEDIUM, CoapConstants.SOURCE_LINGER_MS)
}

object CoapSinkConfig {
  val base: ConfigDef = CoapConfig.config

  val config: ConfigDef = base
    .define(CoapConstants.ERROR_POLICY, Type.STRING, CoapConstants.ERROR_POLICY_DEFAULT, Importance.HIGH, CoapConstants.ERROR_POLICY_DOC,
      "Error", 1, ConfigDef.Width.LONG, CoapConstants.ERROR_POLICY)
    .define(CoapConstants.NBR_OF_RETRIES, Type.INT, CoapConstants.NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, CoapConstants.NBR_OF_RETRIES_DOC,
      "Error", 2, ConfigDef.Width.LONG, CoapConstants.NBR_OF_RETRIES)
    .define(CoapConstants.ERROR_RETRY_INTERVAL, Type.INT, CoapConstants.ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, CoapConstants.ERROR_RETRY_INTERVAL_DOC,
      "Error", 3, ConfigDef.Width.LONG, CoapConstants.ERROR_RETRY_INTERVAL)
}

object CoapSourceConfig {
  val config: ConfigDef = CoapConfig.config
}

case class CoapSinkConfig(props: util.Map[String, String])
  extends BaseConfig(CoapConstants.CONNECTOR_PREFIX, CoapSinkConfig.config, props)
    with CoapConfigBase {

}

case class CoapSourceConfig(props: util.Map[String, String])
  extends BaseConfig(CoapConstants.CONNECTOR_PREFIX, CoapSourceConfig.config, props)
    with CoapConfigBase


case class CoapConfig(props: util.Map[String, String])
  extends BaseConfig(CoapConstants.CONNECTOR_PREFIX, CoapConfig.config, props)
  with CoapConfigBase

trait CoapConfigBase
  extends KcqlSettings
  with DatabaseSettings
  with NumberRetriesSettings
  with ErrorPolicySettings
  with SSLSettings
  with ConnectionSettings
  with UserSettings