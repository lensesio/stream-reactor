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

import java.io.{File, FileInputStream, InputStreamReader}
import java.security._
import java.security.spec.{PKCS8EncodedKeySpec, X509EncodedKeySpec}

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicy
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.util.io.pem.PemReader

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
                       kcql: Kcql,
                       retries: Option[Int],
                       errorPolicy: Option[ErrorPolicy],
                       target: String,
                       bindHost: String,
                       bindPort: Int,
                       identity: String,
                       secret: Password,
                       privateKey: Option[PrivateKey],
                       publicKey: Option[PublicKey]
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

    val kcql = config.getKCQL
    val sink = if (config.isInstanceOf[CoapSinkConfig]) true else false
    val errorPolicy = if (sink) Some(config.getErrorPolicy) else None
    val retries = if (sink) Some(config.getNumberRetries) else None

    val bindPort = config.getPort
    val bindHost = config.getHost

    val identity = config.getUsername
    val secret = config.getSecret

    val privateKeyPathStr = config.getString(CoapConstants.COAP_PRIVATE_KEY_FILE)
    val publicKeyPathStr = config.getString(CoapConstants.COAP_PUBLIC_KEY_FILE)

    val factory = KeyFactory.getInstance("RSA")
    Security.addProvider(new BouncyCastleProvider)

    val privateKey = if (privateKeyPathStr.nonEmpty) {
      if (!new File(privateKeyPathStr).exists()) {
        throw new ConfigException(s"${CoapConstants.COAP_PRIVATE_KEY_FILE} is invalid. Can't locate $privateKeyPathStr")
      }
      Some(loadPrivateKey(privateKeyPathStr, factory))
    } else {
      None
    }

    val publicKey = if (publicKeyPathStr.nonEmpty) {
      if (!new File(publicKeyPathStr).exists()) {
        throw new ConfigException(s"${CoapConstants.COAP_PUBLIC_KEY_FILE} is invalid. Can't locate $publicKeyPathStr")
      }
      Some(loadPublicKey(publicKeyPathStr, factory))
    } else {
      None
    }

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
        bindPort,
        identity,
        secret,
        privateKey,
        publicKey
      )
    )
  }

  def loadPrivateKey(path: String, factory: KeyFactory) : PrivateKey = {
    val pemReader = getPemReader(path)
    val content = new PKCS8EncodedKeySpec(pemReader.readPemObject().getContent)
    factory.generatePrivate(content)
  }

  def loadPublicKey(path: String, factory: KeyFactory): PublicKey = {
    val pemReader = getPemReader(path)
    val content = new X509EncodedKeySpec(pemReader.readPemObject().getContent)
    factory.generatePublic(content)
  }

  def getPemReader(path: String) = new PemReader(new InputStreamReader(new FileInputStream(path)))

}