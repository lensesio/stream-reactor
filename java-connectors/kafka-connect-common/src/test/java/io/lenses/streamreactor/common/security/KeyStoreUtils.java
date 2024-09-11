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

import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.FileOutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

@Slf4j
public class KeyStoreUtils {

  private static final String COMMON_NAME = "CN";
  private static final String DIRECTORY_SECURITY = "security";
  private static final String ALGORITHM_RSA = "RSA";
  private static final String PROVIDER_BC = "BC";
  private static final int KEY_SIZE = 2048;
  private static final int DAYS_IN_YEAR = 365;
  private static final String SIGNER_SIGNATURE_ALGORITHM = "SHA256WithRSAEncryption";
  private static final String KEYSTORE_TYPE = "JKS";
  private static final String KEYSTORE_FILE = "keystore.jks";
  private static final String TRUSTSTORE_FILE = "truststore.jks";
  private static final String TEST_ALIAS = "alias";

  static {
    Security.addProvider(new BouncyCastleProvider());
  }

  public static Path createKeystore(String commonName, String keyStorePassword, String trustStorePassword)
      throws Exception {
    Path tmpDir = Files.createTempDirectory(DIRECTORY_SECURITY);

    KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance(ALGORITHM_RSA, PROVIDER_BC);
    keyPairGen.initialize(KEY_SIZE);
    KeyPair keyPair = keyPairGen.generateKeyPair();

    Date notBefore = new Date(Instant.now().toEpochMilli());
    Date notAfter = new Date(Instant.now().plus(Duration.ofDays(DAYS_IN_YEAR)).toEpochMilli());

    SubjectPublicKeyInfo publicKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());

    X509v3CertificateBuilder certBuilder =
        new X509v3CertificateBuilder(
            new X500Name(COMMON_NAME + "=" + commonName),
            BigInteger.valueOf(System.currentTimeMillis()),
            notBefore,
            notAfter,
            new X500Name(COMMON_NAME + "=" + commonName),
            publicKeyInfo
        );

    JcaContentSignerBuilder contentSignerBuilder =
        new JcaContentSignerBuilder(SIGNER_SIGNATURE_ALGORITHM).setProvider(PROVIDER_BC);
    X509Certificate certificate =
        new JcaX509CertificateConverter().setProvider(PROVIDER_BC)
            .getCertificate(certBuilder.build(contentSignerBuilder.build(keyPair.getPrivate())));

    createAndSaveKeystore(tmpDir, keyStorePassword, certificate, (RSAPrivateKey) keyPair.getPrivate());
    createAndSaveTruststore(tmpDir, trustStorePassword, certificate);

    log.info("container -> Creating keystore at " + tmpDir);
    return tmpDir;
  }

  private static String createAndSaveKeystore(Path tmpDir, String password, X509Certificate certificate,
      RSAPrivateKey privateKey) throws Exception {
    KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
    keyStore.load(null, password.toCharArray());

    keyStore.setKeyEntry(TEST_ALIAS, privateKey, password.toCharArray(), new java.security.cert.Certificate[]{
        certificate});

    String keyStorePath = tmpDir.resolve(KEYSTORE_FILE).toString();
    try (FileOutputStream keyStoreOutputStream = new FileOutputStream(keyStorePath)) {
      keyStore.store(keyStoreOutputStream, password.toCharArray());
    }

    return keyStorePath;
  }

  private static String createAndSaveTruststore(Path tmpDir, String password, X509Certificate certificate)
      throws Exception {
    KeyStore trustStore = KeyStore.getInstance(KEYSTORE_TYPE);
    trustStore.load(null, password.toCharArray());

    trustStore.setCertificateEntry(TEST_ALIAS, certificate);
    String trustStorePath = tmpDir.resolve(TRUSTSTORE_FILE).toString();

    try (FileOutputStream trustStoreOutputStream = new FileOutputStream(trustStorePath)) {
      trustStore.store(trustStoreOutputStream, password.toCharArray());
    }

    return trustStorePath;
  }
}
