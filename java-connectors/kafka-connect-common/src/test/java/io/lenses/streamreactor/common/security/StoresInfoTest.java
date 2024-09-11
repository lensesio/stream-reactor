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
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.nio.file.Path;

import static cyclops.control.Either.right;
import static cyclops.control.Option.none;
import static cyclops.control.Option.some;
import static io.lenses.streamreactor.test.utils.EitherValues.assertLeft;
import static io.lenses.streamreactor.test.utils.EitherValues.assertRight;
import static io.lenses.streamreactor.test.utils.EitherValues.getLeft;
import static io.lenses.streamreactor.test.utils.EitherValues.getRight;
import static io.lenses.streamreactor.test.utils.OptionValues.getValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StoresInfoTest {

  private static final String KEY_OR_TRUST_MANAGER_ALGORITHM = "PKIX";
  private static final String STORE_PASSWORD = "changeIt";

  private final Path keystoreDir = KeyStoreUtils.createKeystore("TestCommonName", STORE_PASSWORD, STORE_PASSWORD);
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
    val storeInfo = new KeyStoreInfo(keystoreFile, StoreType.JKS, STORE_PASSWORD, none());
    val storesInfo = new StoresInfo(none(), some(storeInfo));

    val sslContext = getRight(storesInfo.toSslContext());

    assertEquals("TLS", getValue(sslContext).getProtocol());
  }

  @Test
  void testToSslContextWithTrustStoreDefined() {
    val storeInfo = new TrustStoreInfo(keystoreFile, StoreType.JKS, some(STORE_PASSWORD), none());
    val storesInfo = new StoresInfo(some(storeInfo), none());

    val sslContext = getRight(storesInfo.toSslContext());

    assertEquals("TLS", getValue(sslContext).getProtocol());
  }

  @Test
  void testToSslContextWithBothStoresDefined() {
    val keyStoreInfo = new KeyStoreInfo(keystoreFile, StoreType.JKS, STORE_PASSWORD, none());
    val trustStoreInfo = new TrustStoreInfo(truststoreFile, StoreType.JKS, some(STORE_PASSWORD), none());
    val storesInfo = new StoresInfo(some(trustStoreInfo), some(keyStoreInfo));

    val sslContext = getRight(storesInfo.toSslContext());

    assertEquals("TLS", getValue(sslContext).getProtocol());
  }

  @Test
  void testToSslContextThrowsFileNotFoundExceptionForInvalidKeyStorePath() {
    val keyStoreInfo = new KeyStoreInfo("/invalid/path/to/keystore", StoreType.JKS, STORE_PASSWORD, none());
    val storesInfo = new StoresInfo(none(), some(keyStoreInfo));

    assertEquals(FileNotFoundException.class, getLeft(storesInfo.toSslContext()).getCause().getClass());
  }

  @Test
  void testToSslContextThrowsFileNotFoundExceptionForInvalidTrustStorePath() {
    val trustStoreInfo = new TrustStoreInfo("/invalid/path/to/truststore", StoreType.JKS, some(STORE_PASSWORD), none());
    val storesInfo = new StoresInfo(some(trustStoreInfo), none());

    assertEquals(FileNotFoundException.class, getLeft(storesInfo.toSslContext()).getCause().getClass());

  }

  @Test
  void testStoresInfoCreationFromBaseConfig() {
    BaseConfig mockConfig = mock(BaseConfig.class);
    when(mockConfig.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)).thenReturn("/path/to/truststore");
    when(mockConfig.getString(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG)).thenReturn("JKS");
    when(mockConfig.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).thenReturn(null);
    when(mockConfig.getString(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG)).thenReturn(KEY_OR_TRUST_MANAGER_ALGORITHM);

    when(mockConfig.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)).thenReturn("/path/to/keystore");
    when(mockConfig.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG)).thenReturn("JKS");
    when(mockConfig.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)).thenReturn(new Password(STORE_PASSWORD));
    when(mockConfig.getString(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG)).thenReturn(KEY_OR_TRUST_MANAGER_ALGORITHM);

    val storesInfo = StoresInfo.fromConfig(mockConfig);

    assertRight(storesInfo).isEqualTo(
        new StoresInfo(
            some(new TrustStoreInfo("/path/to/truststore", StoreType.JKS, none(), some(
                KEY_OR_TRUST_MANAGER_ALGORITHM))),
            some(new KeyStoreInfo("/path/to/keystore", StoreType.JKS, STORE_PASSWORD, some(
                KEY_OR_TRUST_MANAGER_ALGORITHM)))
        )
    );
  }

  @Test
  void testStoresInfoCreationFromBaseConfigFailsWithoutTrustStorePassword() {
    BaseConfig mockConfig = mock(BaseConfig.class);
    when(mockConfig.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)).thenReturn("/path/to/truststore");
    when(mockConfig.getString(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG)).thenReturn("JKS");
    when(mockConfig.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).thenReturn(null);

    when(mockConfig.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)).thenReturn("/path/to/keystore");
    when(mockConfig.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG)).thenReturn("JKS");
    when(mockConfig.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)).thenReturn(null);

    val storesInfo = StoresInfo.fromConfig(mockConfig);

    assertLeft(storesInfo).withFailMessage(() -> " Password is required for key store");

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
    assertRight(storesInfo).isEqualTo(
        new StoresInfo(
            none(),
            none()
        )
    );
  }
}
