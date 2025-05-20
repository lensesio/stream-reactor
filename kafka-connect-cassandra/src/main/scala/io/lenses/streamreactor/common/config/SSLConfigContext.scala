/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.common.config

import java.io.FileInputStream
import java.security.KeyStore
import java.security.SecureRandom
import javax.net.ssl._

/**
  * Created by andrew@datamountaineer.com on 14/04/16.
  * stream-reactor
  */

/**
  * For a solution that can be shared between java and scala please see
  * [[io.lenses.streamreactor.common.security.StoresInfo]], which can
  * be used in conjunction with withClientSslSupport in [[org.apache.kafka.common.config.ConfigDef]]
  */
@deprecated("Please use io.lenses.streamreactor.common.security.StoresInfo")
object SSLConfigContext {
  def apply(config: SSLConfig): SSLContext =
    getSSLContext(config)

  /**
    * Get a SSL Connect for a given set of credentials
    *
    * @param config An SSLConfig containing key and truststore credentials
    * @return a SSLContext
    */
  private def getSSLContext(config: SSLConfig): SSLContext = {
    val useClientCertAuth = config.keyStorePath.isDefined && config.keyStorePass.isDefined

    //is client certification authentication set
    val keyManagers: Array[KeyManager] = if (useClientCertAuth) {
      getKeyManagers(config)
    } else {
      Array[KeyManager]()
    }

    val ctx: SSLContext = SSLContext.getInstance("SSL")
    val trustManagers = getTrustManagers(config)
    ctx.init(keyManagers, trustManagers, new SecureRandom())
    ctx
  }

  /**
    * Get an array of Trust Managers
    *
    * @param config An SSLConfig containing key and truststore credentials
    * @return An Array of TrustManagers
    */
  def getTrustManagers(config: SSLConfig): Array[TrustManager] = {
    val tsf = new FileInputStream(config.trustStorePath)
    val ts  = KeyStore.getInstance(config.trustStoreType)
    ts.load(tsf, config.trustStorePass.toCharArray)
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(ts)
    tmf.getTrustManagers
  }

  /**
    * Get an array of Key Managers
    *
    * @param config An SSLConfig containing key and truststore credentials
    * @return An Array of KeyManagers
    */
  def getKeyManagers(config: SSLConfig): Array[KeyManager] = {
    require(config.keyStorePath.nonEmpty, "Key store path is not set!")
    require(config.keyStorePass.nonEmpty, "Key store password is not set!")
    val ksf = new FileInputStream(config.keyStorePath.get)
    val ks  = KeyStore.getInstance(config.keyStoreType)
    ks.load(ksf, config.keyStorePass.get.toCharArray)
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(ks, config.keyStorePass.get.toCharArray)
    kmf.getKeyManagers
  }

}

/**
  * Class for holding key and truststore settings
  */
case class SSLConfig(
  trustStorePath: String,
  trustStorePass: String,
  keyStorePath:   Option[String],
  keyStorePass:   Option[String],
  keyStoreType:   String = "JKS",
  trustStoreType: String = "JKS",
)
