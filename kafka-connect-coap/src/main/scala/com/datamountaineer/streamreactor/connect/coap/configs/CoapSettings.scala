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

import java.io.File

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum}
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.{AbstractConfig, ConfigException}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
case class CoapSetting(uri: String,
                       keyStoreLoc: String,
                       keyStorePass: Password,
                       trustStoreLoc: String,
                       trustStorePass: Password,
                       certs: Array[String],
                       chainKey: String,
                       kcql: Config,
                       retries: Option[Int],
                       errorPolicy: Option[ErrorPolicy],
                       target: String,
                       bindHost: String,
                       bindPort: Int,
                       sink: Boolean
                      )

object CoapSettings {
  def apply(config: AbstractConfig): Set[CoapSetting] = {
    val uri = config.getString(CoapConstants.COAP_URI)
    val keyStoreLoc = config.getString(CoapConstants.COAP_KEY_STORE_PATH)
    val keyStorePass = config.getPassword(CoapConstants.COAP_KEY_STORE_PASS)
    val trustStoreLoc = config.getString(CoapConstants.COAP_TRUST_STORE_PATH)
    val trustStorePass = config.getPassword(CoapConstants.COAP_TRUST_STORE_PASS)
    val certs = config.getList(CoapConstants.COAP_TRUST_CERTS).asScala.toArray
    val certChainKey = config.getString(CoapConstants.COAP_CERT_CHAIN_KEY)

    if (keyStoreLoc != null && keyStoreLoc.nonEmpty && !new File(keyStoreLoc).exists()) {
      throw new ConfigException(s"${CoapConstants.COAP_KEY_STORE_PATH} is invalid. Can't locate $keyStoreLoc")
    }

    if (trustStoreLoc != null && trustStoreLoc.nonEmpty && !new File(trustStoreLoc).exists()) {
      throw new ConfigException(s"${CoapConstants.COAP_TRUST_STORE_PATH} is invalid. Can't locate $trustStoreLoc")
    }

    val raw = config.getString(CoapConstants.COAP_KCQL)
    require(raw != null && !raw.isEmpty, s"No ${CoapConstants.COAP_KCQL} provided!")
    val routes = raw.split(";").map(r => Config.parse(r)).toSet

    val sink = if (config.isInstanceOf[CoapSinkConfig]) true else false
    val bindPort = if (sink) config.getInt(CoapConstants.COAP_DTLS_BIND_PORT) else config.getInt(CoapConstants.COAP_DTLS_BIND_PORT)
    val bindHost = if (sink) config.getString(CoapConstants.COAP_DTLS_BIND_HOST) else config.getString(CoapConstants.COAP_DTLS_BIND_HOST)

    val errorPolicyE = if (sink) Some(ErrorPolicyEnum.withName(config.getString(CoapConstants.ERROR_POLICY).toUpperCase)) else None
    val errorPolicy = if (sink) Some(ErrorPolicy(errorPolicyE.get)) else None
    val retries = if (sink) Some(config.getInt(CoapConstants.NBR_OF_RETRIES).toInt) else None

    routes.map(r =>
      CoapSetting(uri,
        keyStoreLoc,
        keyStorePass,
        trustStoreLoc,
        trustStorePass,
        certs,
        certChainKey,
        r,
        retries,
        errorPolicy,
        if (sink) r.getTarget else r.getSource,
        bindHost,
        bindPort,
        sink
      )
    )
  }
}