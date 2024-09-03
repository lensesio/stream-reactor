package io.lenses.streamreactor.common.security;

import cyclops.control.Either;
import cyclops.control.Option;
import cyclops.control.Try;
import cyclops.data.Seq;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import static java.util.function.Function.identity;

@AllArgsConstructor
@Getter
public class StoresInfo {
    private Option<StoreInfo> maybeTrustStore;
    private Option<StoreInfo> maybeKeyStore;

    private Try<KeyStore, Exception> getJksStore(String path, Option<String> storeType, Option<String> password) {
        return Try.withCatch(
                ()-> KeyStore.getInstance(storeType.map(String::toUpperCase).orElse("JKS")),
                        GeneralSecurityException.class
                )
                .forEach3(
                        (KeyStore keyStore) -> Try.withCatch(
                                () -> new FileInputStream(path),
                                FileNotFoundException.class
                        ),
                        (KeyStore keyStore, FileInputStream truststoreStream) -> Try.runWithCatch(
                                () -> keyStore.load(truststoreStream, (password.orElse("")).toCharArray()),
                                IOException.class,
                                NoSuchAlgorithmException.class,
                                CertificateException.class
                        ),
                (KeyStore keyStore, FileInputStream truststoreStream, Void ignore) -> Try.success(keyStore));

    }

    public Either<Exception, Option<SSLContext>> toSslContext() {


        val maybeTrustFactory = maybeTrustStore.map(
                trustStore ->
                        trustManagers(
                                trustStore.getStorePath(),
                                trustStore.getStoreType(),
                                trustStore.getStorePassword()
                        )
        );

        val maybeKeyFactory = maybeKeyStore.flatMap(
                keyStore ->
                        keyManagers(
                                keyStore.getStorePath(),
                                keyStore.getStoreType(),
                                keyStore.getStorePassword()
                        ).toOption()
        );


        val newSeq = Seq.of(maybeTrustFactory, maybeKeyFactory).flatMap(identity());
         if (.stream().flatMap(identity()))

             Option.when(maybeTrustFactory.nonEmpty || maybeKeyFactory.nonEmpty) {
            val sslContext = SSLContext.getInstance("TLS")
            sslContext.init(
                    maybeKeyFactory.map(_.getKeyManagers).orNull,
                    maybeTrustFactory.map(_.getTrustManagers).orNull,
                    null,
                    )
            sslContext
        }
    }




    private Try<TrustManagerFactory, GeneralSecurityException> trustManagers(String path, Option<String> storeType, Option<String> password) {
        return getJksStore(path, storeType, password)
                .forEach2(
                        (keyStore) ->
                                Try.withCatch(
                                        () -> {
                                            val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                                                    return tmf;
                                        },
                                        GeneralSecurityException.class
                                ),
                        (keyStore, trustManagerFactory) ->
                                Try.withCatch( () -> {
                                    trustManagerFactory.init(keyStore, GeneralSecurityException.class);
                                    return trustManagerFactory;
                                })
                );

    }

    private Either<KeyStoreException, KeyManagerFactory> keyManagers(String path, Option<String> storeType, Option<String> password) {
        val keyStore = getJksStore(path, storeType, password);

        val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
        keyManagerFactory.init(keyStore, (password.orElse("")).toCharArray());
        return keyManagerFactory;
    }

}
