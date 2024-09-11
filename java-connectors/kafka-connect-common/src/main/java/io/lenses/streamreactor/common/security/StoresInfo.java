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
import io.lenses.streamreactor.common.exception.SecuritySetupException;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.lenses.streamreactor.common.security.StoreType.DEFAULT_STORE_TYPE;

@AllArgsConstructor
@Data
public class StoresInfo {

  private static final String PROTOCOL_TLS = "TLS";

  private Option<TrustStoreInfo> maybeTrustStore;
  private Option<KeyStoreInfo> maybeKeyStore;

  private Try<KeyStore, SecuritySetupException> getJksStore(String path, StoreType storeType, Option<String> password) {
    return Try.withCatch(
        () -> {
          val keyStore = KeyStore.getInstance(storeType.toString());
          val inputStream = new FileInputStream(path);
          keyStore.load(inputStream, password.map(String::toCharArray).orElse(null));
          return keyStore;
        },
        Exception.class
    ).mapFailure(ex -> new SecuritySetupException("unable to retrieve keystore", ex));

  }

  public Either<SecuritySetupException, Option<SSLContext>> toSslContext() {

    final Option<Try<TrustManagerFactory, SecuritySetupException>> maybeTrustFactory =
        maybeTrustStore.map(
            trustStore -> trustManagers(
                trustStore.getStorePath(),
                trustStore.getStoreType(),
                trustStore.getStorePassword()
            )
        );

    final Option<Try<KeyManagerFactory, SecuritySetupException>> maybeKeyFactory =
        maybeKeyStore.map(
            keyStore -> keyManagers(
                keyStore.getStorePath(),
                keyStore.getStoreType(),
                keyStore.getStorePassword()
            )
        );

    val failures =
        Stream.of(
            maybeTrustFactory.filter(Try::isFailure).flatMap(Try::failureGet).stream(),
            maybeKeyFactory.filter(Try::isFailure).flatMap(Try::failureGet).stream()
        )
            .flatMap(Function.identity())
            .collect(Collectors.toUnmodifiableList());

    val maybeFailure =
        Option.fromOptional(
            failures
                .stream()
                .findFirst()
        );

    return maybeFailure
        .toEither(getAndInitSslContext(maybeKeyFactory, maybeTrustFactory))
        .swap()
        .fold(Either::left, either -> either.fold(Either::left, Either::right));

  }

  private static Either<SecuritySetupException, Option<SSLContext>> getAndInitSslContext(
      Option<Try<KeyManagerFactory, SecuritySetupException>> maybeKeyFactory,
      Option<Try<TrustManagerFactory, SecuritySetupException>> maybeTrustFactory
  ) {
    return Try.withCatch(() -> {
      // If either factory is present, initialize SSLContext
      if (maybeKeyFactory.isPresent() || maybeTrustFactory.isPresent()) {
        val sslContext = SSLContext.getInstance(PROTOCOL_TLS);
        sslContext.init(
            maybeKeyFactory.flatMap(Try::toOption).map(KeyManagerFactory::getKeyManagers).orElse(null),
            maybeTrustFactory.flatMap(Try::toOption).map(TrustManagerFactory::getTrustManagers).orElse(null),
            null
        );
        return Option.of(sslContext);
      }
      return Option.<SSLContext>none();
    }).mapFailure(ex -> new SecuritySetupException("unable to retrieve keystore", ex))
        .toEither();
  }

  private Try<TrustManagerFactory, SecuritySetupException> trustManagers(String path, StoreType storeType,
      Option<String> password) {
    return Try.narrowK(
        Do.forEach(
            TryInstances.<SecuritySetupException>monad()
        )
            .__(getJksStore(path, storeType, password))
            .__(StoresInfo::getTrustManagerFactoryFromKeyStore)
            .yield(
                (KeyStore keyStore, TrustManagerFactory trustManagerFactory) -> trustManagerFactory
            )
            .unwrap());

  }

  private Try<KeyManagerFactory, SecuritySetupException> keyManagers(String path, StoreType storeType,
      String password) {
    return Try.narrowK(
        Do.forEach(
            TryInstances.<SecuritySetupException>monad()
        )
            .__(getJksStore(path, storeType, Option.of(password)))
            .__(s -> StoresInfo.getKeyManagerFactoryFromKeyStore(s, password))
            .yield(
                (KeyStore keyStore, KeyManagerFactory trustManagerFactory) -> trustManagerFactory
            )
            .unwrap());
  }

  private static Try<TrustManagerFactory, SecuritySetupException> getTrustManagerFactoryFromKeyStore(
      KeyStore keyStore) {
    return Try.withCatch(() -> {
      val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(keyStore);
      return trustManagerFactory;
    }, NoSuchAlgorithmException.class, KeyStoreException.class)
        .mapFailure(ex -> new SecuritySetupException("Unable to get trust manager factory from keystore", ex));
  }

  private static Try<KeyManagerFactory, SecuritySetupException> getKeyManagerFactoryFromKeyStore(KeyStore keyStore,
      String password) {
    return Try.withCatch(() -> {
      val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(keyStore, password.toCharArray());
      return keyManagerFactory;
    }, NoSuchAlgorithmException.class, KeyStoreException.class, UnrecoverableKeyException.class)
        .mapFailure(ex -> new SecuritySetupException("Unable to get trust manager factory from truststore", ex));
  }

  public static Either<SecuritySetupException, StoresInfo> fromConfig(AbstractConfig config) {
    val trustStore = configToTrustStoreInfo(config);
    val keyStore = configToKeyStoreInfo(config);

    val failures =
        Stream.of(trustStore, keyStore)
            .flatMap(option -> option.stream().flatMap(either -> either.swap().stream()))
            .collect(Collectors.toUnmodifiableList());

    return failures.isEmpty()
        ? Either.right(new StoresInfo(
            trustStore.flatMap(Either::toOption),
            keyStore.flatMap(Either::toOption)
        ))
        : Either.left(failures.iterator().next());
  }

  private static Option<Either<SecuritySetupException, TrustStoreInfo>> configToTrustStoreInfo(AbstractConfig config) {
    return Option.ofNullable(config.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG))
        .map(storePath -> fromConfigOption(config, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG)
            .map(storeType -> {
              val storePassword =
                  Option.ofNullable(config.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG))
                      .map(Password::value);
              return new TrustStoreInfo(storePath, storeType, storePassword);
            }
            ));
  }

  private static Option<Either<SecuritySetupException, KeyStoreInfo>> configToKeyStoreInfo(
      AbstractConfig config
  ) {
    return Option.ofNullable(config.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
        .flatMap(storePath -> fromConfigOption(config, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG)
            .map(storeType -> Option.ofNullable(config.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG))
                .map(Password::value)
                .toEither(new SecuritySetupException("Password is required for key store"))
                .map(pw -> new KeyStoreInfo(storePath, storeType, pw))
            )
            .toOption()
        );
  }

  private static Either<SecuritySetupException, StoreType> fromConfigOption(AbstractConfig config, String configKey) {
    return Option
        .fromNullable(config.getString(configKey))
        .map(StoreType::valueOfCaseInsensitive)
        .orElse(Either.right(DEFAULT_STORE_TYPE));
  }

}
