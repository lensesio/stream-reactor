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

import java.io.File

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password

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
                       kcql : Config,
                       retries: Int,
                       errorPolicy: ErrorPolicy,
                       target : String,
                       sink: Boolean)

object CoapSettings {
  def apply(config: CoapConfig, sink: Boolean): Set[CoapSetting] = {
    val uri = config.getString(CoapConfig.COAP_URI)
    val keyStoreLoc = config.getString(CoapConfig.COAP_KEY_STORE_PATH)
    val keyStorePass = config.getPassword(CoapConfig.COAP_KEY_STORE_PASS)
    val trustStoreLoc = config.getString(CoapConfig.COAP_TRUST_STORE_PATH)
    val trustStorePass = config.getPassword(CoapConfig.COAP_TRUST_STORE_PASS)
    val certs = config.getList(CoapConfig.COAP_TRUST_CERTS).asScala.toArray
    val certChainKey = config.getString(CoapConfig.COAP_CERT_CHAIN_KEY)

    if (keyStoreLoc.nonEmpty && !new File(keyStoreLoc).exists()) {
      throw new ConfigException(s"${CoapConfig.COAP_KEY_STORE_PATH} is invalid. Can't locate $keyStoreLoc")
    }

    if (trustStoreLoc.nonEmpty && !new File(trustStoreLoc).exists()) {
      throw new ConfigException(s"${CoapConfig.COAP_TRUST_STORE_PATH} is invalid. Can't locate $trustStoreLoc")
    }

    val raw = config.getString(CoapConfig.COAP_KCQL)
    require(raw != null && !raw.isEmpty, s"No ${CoapConfig.COAP_KCQL} provided!")
    val routes = raw.split(";").map(r => Config.parse(r)).toSet

    val retries = config.getInt(CoapConfig.NBR_OF_RETRIES)
    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(CoapConfig.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)

    routes.map(r =>
                  new CoapSetting(uri,
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
                  sink)
    )
  }
}