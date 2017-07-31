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
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicy
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.ConfigException

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
                       bindPort: Int
                      )

object CoapSettings {

  def apply(config: CoapConfigBase): Set[CoapSetting] = {
    val uri = config.getUri

    val keyStoreLoc = config.getKeyStorePath
    val keyStorePass = config.getKeyStorePass
    val trustStoreLoc = config.getTrustStorePath
    val trustStorePass = config.getTrustStorePass

    val certs = config.getCertificates.asScala.toArray
    val certChainKey = config.getCertificateKeyChain

    if (keyStoreLoc != null && keyStoreLoc.nonEmpty && !new File(keyStoreLoc).exists()) {
      throw new ConfigException(s"${CoapConstants.COAP_KEY_STORE_PATH} is invalid. Can't locate $keyStoreLoc")
    }

    if (trustStoreLoc != null && trustStoreLoc.nonEmpty && !new File(trustStoreLoc).exists()) {
      throw new ConfigException(s"${CoapConstants.COAP_TRUST_STORE_PATH} is invalid. Can't locate $trustStoreLoc")
    }

    val raw = config.getString(CoapConstants.COAP_KCQL)
    require(raw != null && !raw.isEmpty, s"No ${CoapConstants.COAP_KCQL} provided!")

    val kcql = config.getKCQL
    val sink = if (config.isInstanceOf[CoapSinkConfig]) true else false
    val errorPolicy= if (sink) Some(config.getErrorPolicy) else None
    val retries = if (sink) Some(config.getRetryInterval) else None

    val bindPort = config.getBindPort
    val bindHost = config.getBindHost

    kcql.map(k =>
      CoapSetting(uri,
        keyStoreLoc,
        keyStorePass,
        trustStoreLoc,
        trustStorePass,
        certs,
        certChainKey,
        k,
        retries,
        errorPolicy,
        if (sink) k.getTarget else k.getSource,
        bindHost,
        bindPort
      )
    )
  }
}