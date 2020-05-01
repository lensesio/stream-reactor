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

package com.datamountaineer.streamreactor.connect.mqtt.source

import java.io.FileReader
import java.security.{KeyStore, Security}

import com.typesafe.scalalogging.StrictLogging
import javax.net.ssl.{KeyManagerFactory, SSLContext, SSLSocketFactory, TrustManagerFactory}
import org.bouncycastle.cert.X509CertificateHolder
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.openssl.jcajce.{JcaPEMKeyConverter, JcePEMDecryptorProviderBuilder}
import org.bouncycastle.openssl.{PEMEncryptedKeyPair, PEMKeyPair, PEMParser}


object MqttSSLSocketFactory extends StrictLogging {
  def apply(caCrtFile: String,
            crtFile: String,
            keyFile: String,
            password: String): SSLSocketFactory = {
    try {

      /**
        * Add BouncyCastle as a Security Provider
        */
      Security.addProvider(new BouncyCastleProvider)
      val certificateConverter = new JcaX509CertificateConverter().setProvider("BC")
      /**
        * Load Certificate Authority (CA) certificate
        */
      var reader = new PEMParser(new FileReader(caCrtFile))
      val caCertHolder = reader.readObject.asInstanceOf[X509CertificateHolder]
      reader.close()
      val caCert = certificateConverter.getCertificate(caCertHolder)

      /**
        * Load client certificate
        */
      reader = new PEMParser(new FileReader(crtFile))
      val certHolder = reader.readObject.asInstanceOf[X509CertificateHolder]
      reader.close()
      val cert = certificateConverter.getCertificate(certHolder)

      /**
        * Load client private key
        */
      reader = new PEMParser(new FileReader(keyFile))
      val keyObject: Any = reader.readObject
      reader.close()

      val provider = new JcePEMDecryptorProviderBuilder().build(password.toCharArray)
      val keyConverter = new JcaPEMKeyConverter().setProvider("BC")
      val key = keyObject match {
        case pair: PEMEncryptedKeyPair => keyConverter.getKeyPair(pair.decryptKeyPair(provider))
        case _ => keyConverter.getKeyPair(keyObject.asInstanceOf[PEMKeyPair])
      }
      /**
        * CA certificate is used to authenticate server
        */
      val caKeyStore = KeyStore.getInstance(KeyStore.getDefaultType)
      caKeyStore.load(null, null)
      caKeyStore.setCertificateEntry("ca-certificate", caCert)
      val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      trustManagerFactory.init(caKeyStore)
      /**
        * Client key and certificates are sent to server so it can authenticate the client
        */
      val clientKeyStore = KeyStore.getInstance(KeyStore.getDefaultType)
      clientKeyStore.load(null, null)
      clientKeyStore.setCertificateEntry("certificate", cert)
      clientKeyStore.setKeyEntry("private-key", key.getPrivate, password.toCharArray, Array(cert))
      val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      keyManagerFactory.init(clientKeyStore, password.toCharArray)
      /**
        * Create SSL socket factory
        */
      val context = SSLContext.getInstance("TLSv1.2")
      context.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, null)

      /**
        * Return the newly created socket factory object
        */
      context.getSocketFactory
    }
    catch {
      case e: Exception =>
        logger.warn(e.getMessage, e)
        null
    }
  }
}
