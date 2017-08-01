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
package com.datamountaineer.streamreactor.connect.cassandra.cdc.config

import java.io.FileInputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl._

/**
  * Class for holding key and truststore settings
  **/
case class SSLConfig(trustStorePath: String,
                     trustStorePass: String,
                     keyStorePath: Option[String],
                     keyStorePass: Option[String],
                     useClientCert: Boolean = false,
                     keyStoreType: String = "JKS",
                     trustStoreType: String = "JKS") {
  /**
    * Get an array of Trust Managers
    *
    * @return An Array of TrustManagers
    **/
  def getTrustManagers(): Array[TrustManager] = {
    val tsf = new FileInputStream(trustStorePath)
    try {
      val ts = KeyStore.getInstance(trustStoreType)
      ts.load(tsf, trustStorePass.toCharArray)
      val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      tmf.init(ts)
      tmf.getTrustManagers
    }
    finally {
      tsf.close()
    }
  }

  /**
    * Get an array of Key Managers
    *
    * @return An Array of KeyManagers
    **/
  def getKeyManagers(): Array[KeyManager] = {
    require(keyStorePath.nonEmpty, "Key store path is not set!")
    require(keyStorePass.nonEmpty, "Key store password is not set!")
    val ksf = new FileInputStream(keyStorePath.get)
    val ks = KeyStore.getInstance(keyStoreType)
    ks.load(ksf, keyStorePass.get.toCharArray)
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(ks, keyStorePass.get.toCharArray)
    kmf.getKeyManagers
  }
}

object SSLConfig {

  def apply(connect: CassandraConnect): Option[SSLConfig] = {
    val ssl = connect.getBoolean(CassandraConnect.SSL_ENABLED).asInstanceOf[Boolean]
    if (ssl) {
      Some(
        SSLConfig(
          trustStorePath = connect.getString(CassandraConnect.TRUST_STORE_PATH),
          trustStorePass = connect.getPassword(CassandraConnect.TRUST_STORE_PASSWD).value,
          keyStorePath = Some(connect.getString(CassandraConnect.KEY_STORE_PATH)),
          keyStorePass = Some(connect.getPassword(CassandraConnect.KEY_STORE_PASSWD).value),
          useClientCert = connect.getBoolean(CassandraConnect.USE_CLIENT_AUTH),
          keyStoreType = connect.getString(CassandraConnect.KEY_STORE_TYPE),
          trustStoreType = connect.getString(CassandraConnect.TRUST_STORE_TYPE)
        )
      )
    } else None
  }

  implicit def convertToSSLContext(sslConfig: SSLConfig): SSLContext = {
    //is client certification authentication set
    val keyManagers: Array[KeyManager] = {
      if (sslConfig.useClientCert) sslConfig.getKeyManagers()
      else Array.empty[KeyManager]
    }

    val ctx: SSLContext = SSLContext.getInstance("SSL")
    ctx.init(keyManagers, sslConfig.getTrustManagers(), new SecureRandom())
    ctx
  }
}