/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.common.security;

import io.lenses.streamreactor.common.config.base.BaseConfig;
import lombok.val;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.nio.file.Path;

import static cyclops.control.Either.right;
import static cyclops.control.Option.none;
import static cyclops.control.Option.some;
import static io.lenses.streamreactor.test.utils.EitherValues.getLeft;
import static io.lenses.streamreactor.test.utils.EitherValues.getRight;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StoresInfoTest {

  private final String password = "changeIt";
  private final Path keystoreDir = KeyStoreUtils.createKeystore("TestCommonName", password, password);
  private final String keystoreFile = keystoreDir.toAbsolutePath() + "/keystore.jks";
  private final String truststoreFile = keystoreDir.toAbsolutePath() + "/truststore.jks";

  StoresInfoTest() throws Exception {
  }

  @Test
  void testToSslContextWithBothNone() {
    val storesInfo = new StoresInfo(none(), none());
    assertEquals(right(none()), storesInfo.toSslContext());
  }

  @Test
  void testToSslContextWithKeyStoreDefined() {
    val storeInfo = new StoreInfo(keystoreFile, some("JKS"), some(password));
    val storesInfo = new StoresInfo(none(), some(storeInfo));

    val sslContext = getRight(storesInfo.toSslContext());

    assertTrue(sslContext.isPresent());
    assertTrue(sslContext.filter(e -> e.getProtocol().equals("TLS")).isPresent());
  }

  @Test
  void testToSslContextWithTrustStoreDefined() {
    val storeInfo = new StoreInfo(keystoreFile, some("JKS"), some(password));
    val storesInfo = new StoresInfo(some(storeInfo), none());

    val sslContext = getRight(storesInfo.toSslContext());

    assertTrue(sslContext.isPresent());
    assertTrue(sslContext.filter(e -> e.getProtocol().equals("TLS")).isPresent());
  }

  @Test
  void testToSslContextWithBothStoresDefined() {
    val keyStoreInfo = new StoreInfo(keystoreFile, some("JKS"), some(password));
    val trustStoreInfo = new StoreInfo(truststoreFile, some("JKS"), some(password));
    val storesInfo = new StoresInfo(some(trustStoreInfo), some(keyStoreInfo));

    val sslContext = getRight(storesInfo.toSslContext());

    assertTrue(sslContext.isPresent());
    assertTrue(sslContext.filter(e -> e.getProtocol().equals("TLS")).isPresent());
  }

  @Test
  void testToSslContextThrowsFileNotFoundExceptionForInvalidKeyStorePath() {
    val keyStoreInfo = new StoreInfo("/invalid/path/to/keystore", some("JKS"), some(password));
    val storesInfo = new StoresInfo(none(), some(keyStoreInfo));

    assertEquals(FileNotFoundException.class, getLeft(storesInfo.toSslContext()).getClass());
  }

  @Test
  void testToSslContextThrowsFileNotFoundExceptionForInvalidTrustStorePath() {
    val trustStoreInfo = new StoreInfo("/invalid/path/to/truststore", some("JKS"), some(password));
    val storesInfo = new StoresInfo(some(trustStoreInfo), none());

    assertEquals(FileNotFoundException.class, getLeft(storesInfo.toSslContext()).getClass());

  }

  @Test
  void testStoresInfoCreationFromBaseConfig() {
    BaseConfig mockConfig = mock(BaseConfig.class);
    when(mockConfig.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)).thenReturn("/path/to/truststore");
    when(mockConfig.getString(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG)).thenReturn("JKS");
    when(mockConfig.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).thenReturn(null);

    when(mockConfig.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)).thenReturn("/path/to/keystore");
    when(mockConfig.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG)).thenReturn("JKS");
    when(mockConfig.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)).thenReturn(null);

    val storesInfo = StoresInfo.fromConfig(mockConfig);

    assertEquals(
        some(new StoreInfo("/path/to/truststore", some("JKS"), none())),
        storesInfo.getMaybeTrustStore()
    );
    assertEquals(
        some(new StoreInfo("/path/to/keystore", some("JKS"), none())),
        storesInfo.getMaybeKeyStore()
    );
  }

  @Test
  void testStoresInfoCreationWithNoneValuesWithMissingConfigs() {
    BaseConfig mockConfig = mock(BaseConfig.class);
    when(mockConfig.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)).thenReturn(null);
    when(mockConfig.getString(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG)).thenReturn(null);
    when(mockConfig.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).thenReturn(null);

    when(mockConfig.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)).thenReturn(null);
    when(mockConfig.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG)).thenReturn(null);
    when(mockConfig.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)).thenReturn(null);

    val storesInfo = StoresInfo.fromConfig(mockConfig);
    assertEquals(none(), storesInfo.getMaybeKeyStore());
    assertEquals(none(), storesInfo.getMaybeTrustStore());
  }
}
