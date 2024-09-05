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
import java.util.Date;

public class KeyStoreUtils {

  static {
    Security.addProvider(new BouncyCastleProvider());
  }

  public static Path createKeystore(String commonName, String keyStorePassword, String trustStorePassword)
      throws Exception {
    Path tmpDir = Files.createTempDirectory("security");

    KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA", "BC");
    keyPairGen.initialize(2048);
    KeyPair keyPair = keyPairGen.generateKeyPair();

    Date notBefore = new Date();
    Date notAfter = new Date(System.currentTimeMillis() + 365L * 24 * 60 * 60 * 1000);

    SubjectPublicKeyInfo publicKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());

    X509v3CertificateBuilder certBuilder =
        new X509v3CertificateBuilder(
            new X500Name("CN=" + commonName),
            BigInteger.valueOf(System.currentTimeMillis()),
            notBefore,
            notAfter,
            new X500Name("CN=" + commonName),
            publicKeyInfo
        );

    JcaContentSignerBuilder contentSignerBuilder =
        new JcaContentSignerBuilder("SHA256WithRSAEncryption").setProvider("BC");
    X509Certificate certificate =
        new JcaX509CertificateConverter().setProvider("BC")
            .getCertificate(certBuilder.build(contentSignerBuilder.build(keyPair.getPrivate())));

    createAndSaveKeystore(tmpDir, keyStorePassword, certificate, (RSAPrivateKey) keyPair.getPrivate());
    createAndSaveTruststore(tmpDir, trustStorePassword, certificate);

    System.out.println("container -> Creating keystore at " + tmpDir);
    return tmpDir;
  }

  private static String createAndSaveKeystore(Path tmpDir, String password, X509Certificate certificate,
      RSAPrivateKey privateKey) throws Exception {
    KeyStore keyStore = KeyStore.getInstance("JKS");
    keyStore.load(null, password.toCharArray());

    keyStore.setKeyEntry("alias", privateKey, password.toCharArray(), new java.security.cert.Certificate[]{
        certificate});

    String keyStorePath = tmpDir.resolve("keystore.jks").toString();
    try (FileOutputStream keyStoreOutputStream = new FileOutputStream(keyStorePath)) {
      keyStore.store(keyStoreOutputStream, password.toCharArray());
    }

    return keyStorePath;
  }

  private static String createAndSaveTruststore(Path tmpDir, String password, X509Certificate certificate)
      throws Exception {
    KeyStore trustStore = KeyStore.getInstance("JKS");
    trustStore.load(null, password.toCharArray());

    trustStore.setCertificateEntry("alias", certificate);
    String trustStorePath = tmpDir.resolve("truststore.jks").toString();

    try (FileOutputStream trustStoreOutputStream = new FileOutputStream(trustStorePath)) {
      trustStore.store(trustStoreOutputStream, password.toCharArray());
    }

    return trustStorePath;
  }
}
