/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.security

import io.lenses.streamreactor.common.config.base.traits.BaseConfig
import org.apache.kafka.common.config.SslConfigs
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.FileNotFoundException
import java.nio.file.Path

class StoresInfoTest extends AnyFunSuite with Matchers with MockitoSugar with BeforeAndAfterAll {

  private val password = "changeIt"
  private val keystoreDir: Path = KeyStoreUtils.createKeystore("TestCommonName", password, password)
  private val keystoreFile   = keystoreDir.toAbsolutePath.toString + "/keystore.jks"
  private val truststoreFile = keystoreDir.toAbsolutePath.toString + "/truststore.jks"

  test("StoresInfo.toSslContext should return None when both trustStore and keyStore are None") {
    val storesInfo = StoresInfo(None, None)
    storesInfo.toSslContext should be(None)
  }

  test("StoresInfo.toSslContext should return Some(SSLContext) when keyStore is defined") {
    val storeInfo  = StoreInfo(keystoreFile, Some("JKS"), Some(password))
    val storesInfo = StoresInfo(keyStore = Some(storeInfo))

    val sslContext = storesInfo.toSslContext

    sslContext should not be empty
    sslContext.get.getProtocol shouldEqual "TLS"
  }

  test("StoresInfo.toSslContext should return Some(SSLContext) when trustStore is defined") {
    val storeInfo  = StoreInfo(keystoreFile, Some("JKS"), Some(password))
    val storesInfo = StoresInfo(trustStore = Some(storeInfo))

    val sslContext = storesInfo.toSslContext

    sslContext should not be empty
    sslContext.get.getProtocol shouldEqual "TLS"
  }

  test("StoresInfo.toSslContext should return Some(SSLContext) when both keyStore and trustStore are defined") {
    val keyStoreInfo   = StoreInfo(keystoreFile, Some("JKS"), Some(password))
    val trustStoreInfo = StoreInfo(truststoreFile, Some("JKS"), Some(password))
    val storesInfo     = StoresInfo(trustStore = Some(trustStoreInfo), keyStore = Some(keyStoreInfo))

    val sslContext = storesInfo.toSslContext

    sslContext should not be empty
    sslContext.get.getProtocol shouldEqual "TLS"
  }

  test("StoresInfo.toSslContext should throw FileNotFoundException if the keyStore path is incorrect") {
    val keyStoreInfo = StoreInfo("/invalid/path/to/keystore", Some("JKS"), Some(password))
    val storesInfo   = StoresInfo(keyStore = Some(keyStoreInfo))

    assertThrows[FileNotFoundException] {
      storesInfo.toSslContext
    }
  }

  test("StoresInfo.toSslContext should throw FileNotFoundException if the trustStore path is incorrect") {
    val trustStoreInfo = StoreInfo("/invalid/path/to/truststore", Some("JKS"), Some(password))
    val storesInfo     = StoresInfo(trustStore = Some(trustStoreInfo))

    assertThrows[FileNotFoundException] {
      storesInfo.toSslContext
    }
  }

  test("StoresInfo.apply should create StoresInfo with correct values from BaseConfig") {
    val mockConfig: BaseConfig = mock[BaseConfig]
    when(mockConfig.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)).thenReturn("/path/to/truststore")
    when(mockConfig.getString(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG)).thenReturn("JKS")
    when(mockConfig.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).thenReturn(null)

    when(mockConfig.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)).thenReturn("/path/to/keystore")
    when(mockConfig.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG)).thenReturn("JKS")
    when(mockConfig.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)).thenReturn(null)

    val storesInfo = StoresInfo(mockConfig)

    storesInfo.trustStore shouldEqual Some(StoreInfo("/path/to/truststore", Some("JKS"), None))
    storesInfo.keyStore shouldEqual Some(StoreInfo("/path/to/keystore", Some("JKS"), None))
  }

  test("StoresInfo.apply should create StoresInfo with None values if configs are missing") {
    val mockConfig: BaseConfig = mock[BaseConfig]
    when(mockConfig.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)).thenReturn(null)
    when(mockConfig.getString(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG)).thenReturn(null)
    when(mockConfig.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).thenReturn(null)

    when(mockConfig.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)).thenReturn(null)
    when(mockConfig.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG)).thenReturn(null)
    when(mockConfig.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)).thenReturn(null)

    val storesInfo = StoresInfo(mockConfig)

    storesInfo.trustStore shouldEqual None
    storesInfo.keyStore shouldEqual None
  }
}
