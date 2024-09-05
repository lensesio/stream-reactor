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

import cyclops.control.Either;
import cyclops.control.Option;
import cyclops.control.Try;
import cyclops.instances.control.TryInstances;
import cyclops.typeclasses.Do;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;

@AllArgsConstructor
@Data
public class StoresInfo {

  private Option<StoreInfo> maybeTrustStore;
  private Option<StoreInfo> maybeKeyStore;

  private Try<KeyStore, Exception> getJksStore(String path, Option<String> storeType, Option<String> password) {
    return Try.withCatch(
        () -> {
          val keyStore = getKeyStoreInstance(storeType);
          val inputStream = new FileInputStream(path);
          loadKeyStore(password, keyStore, inputStream);
          return keyStore;
        },
        Exception.class
    );

  }

  private static void loadKeyStore(Option<String> password, KeyStore keyStore, FileInputStream truststoreStream)
      throws Exception {
    keyStore.load(truststoreStream, (password.orElse("")).toCharArray());
  }

  private static KeyStore getKeyStoreInstance(Option<String> storeType) throws Exception {
    return KeyStore.getInstance(storeType.map(String::toUpperCase).orElse("JKS"));
  }

  public Either<Exception, Option<SSLContext>> toSslContext() {

    final Option<Try<TrustManagerFactory, Exception>> maybeTrustFactory =
        maybeTrustStore.map(
            trustStore -> trustManagers(
                trustStore.getStorePath(),
                trustStore.getStoreType(),
                trustStore.getStorePassword()
            )
        );

    final Option<Try<KeyManagerFactory, Exception>> maybeKeyFactory =
        maybeKeyStore.map(
            keyStore -> keyManagers(
                keyStore.getStorePath(),
                keyStore.getStoreType(),
                keyStore.getStorePassword()
            )
        );

    if (maybeTrustFactory.isPresent() || maybeKeyFactory.isPresent()) {

      val trustFailure = maybeTrustFactory.filter(Try::isFailure).flatMap(Try::failureGet);
      val keyFailure = maybeKeyFactory.filter(Try::isFailure).flatMap(Try::failureGet);

      if (trustFailure.isPresent() || keyFailure.isPresent()) {
        return Either.left(trustFailure.orElse(keyFailure.orElse(new IllegalStateException(
            "Logic error retrieving trust factories"))));
      } else {

        val trustSuccess = maybeTrustFactory.filter(Try::isSuccess).flatMap(Try::get);
        val keySuccess = maybeKeyFactory.filter(Try::isSuccess).flatMap(Try::get);

        return Try.withCatch(() -> {
          val sslContext = SSLContext.getInstance("TLS");
          sslContext.init(
              keySuccess.map(KeyManagerFactory::getKeyManagers).orElse(null),
              trustSuccess.map(TrustManagerFactory::getTrustManagers).orElse(null),
              null
          );
          return sslContext;
        }
        ).toEither().bimap(l -> l, Option::some);

      }

    } else {
      return Either.right(Option.none());
    }
  }

  private Try<TrustManagerFactory, Exception> trustManagers(String path, Option<String> storeType,
      Option<String> password) {
    return Try.narrowK(
        Do.forEach(
            TryInstances.<Exception>monad()
        )
            .__(getJksStore(path, storeType, password))
            .__((KeyStore keyStore) -> Try.withCatch(
                () -> getTrustManagerFactoryFromKeyStore(keyStore),
                Exception.class
            )
            )
            .yield(
                (KeyStore keyStore, TrustManagerFactory trustManagerFactory) -> trustManagerFactory
            )
            .unwrap());

  }

  private Try<KeyManagerFactory, Exception> keyManagers(String path, Option<String> storeType,
      Option<String> password) {
    return Try.narrowK(
        Do.forEach(
            TryInstances.<Exception>monad()
        )
            .__(getJksStore(path, storeType, password))
            .__((KeyStore keyStore) -> Try.withCatch(
                () -> getKeyManagerFactoryFromKeyStore(keyStore, password),
                Exception.class
            )
            )
            .yield(
                (KeyStore keyStore, KeyManagerFactory trustManagerFactory) -> trustManagerFactory
            )
            .unwrap());
  }

  private static TrustManagerFactory getTrustManagerFactoryFromKeyStore(KeyStore keyStore)
      throws NoSuchAlgorithmException, KeyStoreException {
    val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(keyStore);
    return trustManagerFactory;
  }

  private static KeyManagerFactory getKeyManagerFactoryFromKeyStore(KeyStore keyStore, Option<String> password)
      throws NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException {
    val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, (password.orElse("")).toCharArray());
    return keyManagerFactory;
  }

  public static StoresInfo fromConfig(AbstractConfig config) {
    val trustStore =
        Option.fromNullable(config.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG))
            .map(storePath -> {
              val storeType = Option.fromNullable(config.getString(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
              val storePassword =
                  Option.fromNullable(config.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG))
                      .map(Password::value);
              return new StoreInfo(storePath, storeType, storePassword);
            });
    val keyStore =
        Option.fromNullable(config.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
            .map(storePath -> {
              val storeType = Option.fromNullable(config.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG));
              val storePassword =
                  Option.fromNullable(config.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)).map(Password::value);
              return new StoreInfo(storePath, storeType, storePassword);
            });

    return new StoresInfo(trustStore, keyStore);
  }

}
