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

package com.datamountaineer.streamreactor.connect.coap;

import com.google.common.collect.Lists;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemReader;
import org.eclipse.californium.core.CoapServer;
import org.eclipse.californium.core.network.CoapEndpoint;
import org.eclipse.californium.core.network.Endpoint;
import org.eclipse.californium.core.network.interceptors.MessageTracer;
import org.eclipse.californium.elements.config.Configuration;
import org.eclipse.californium.scandium.DTLSConnector;
import org.eclipse.californium.scandium.config.DtlsConfig;
import org.eclipse.californium.scandium.config.DtlsConnectorConfig;
import org.eclipse.californium.scandium.dtls.CertificateType;
import org.eclipse.californium.scandium.dtls.cipher.CipherSuite;
import org.eclipse.californium.scandium.dtls.cipher.DefaultCipherSuiteSelector;
import org.eclipse.californium.scandium.dtls.pskstore.AdvancedSinglePskStore;
import org.eclipse.californium.scandium.dtls.x509.CertificateConfigurationHelper;
import org.eclipse.californium.scandium.dtls.x509.NewAdvancedCertificateVerifier;
import org.eclipse.californium.scandium.dtls.x509.SingleCertificateProvider;
import org.eclipse.californium.scandium.dtls.x509.StaticNewAdvancedCertificateVerifier;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.List;

public class Server {

  private CoapServer server;
  ClassLoader classLoader = getClass().getClassLoader();
  private static final String TRUST_STORE_PASSWORD = "rootPass";
  private final static String KEY_STORE_PASSWORD = "endPass";
  private static final String KEY_STORE_LOCATION = "certs2/keyStore.jks";
  private static final String TRUST_STORE_LOCATION = "certs2/trustStore.jks";

  Integer secure;
  Integer insecure;
  Integer key;

  public Server(Integer securePort, Integer insecurePort, Integer keyport) {
    //Security.addProvider(new BouncyCastleProvider());
    Security.removeProvider("BC");
    BouncyCastleProvider bouncyCastleProvider = new BouncyCastleProvider();
    Security.insertProviderAt(bouncyCastleProvider, 1);
    server = new CoapServer();
    server.add(new ObservableResource("secure"));
    server.add(new ObservableResource("insecure"));
    server.add(new ObservableResource("key"));

    secure = securePort;
    insecure = insecurePort;
    key = keyport;

    DTLSConnector sslConnector = getConnectorSSL(securePort);
    DTLSConnector keyConnector = getConnectorKeys(key);

    //add secure
    server.addEndpoint(
            new CoapEndpoint
                    .Builder()
                    .setConnector(sslConnector)
                    .build()
    );

    //add key
    server.addEndpoint(
            new CoapEndpoint
                    .Builder()
                    .setConnector(keyConnector)
                    .build()
    );

    //add unsecure
    InetSocketAddress addr = null;
    try {
      addr = new InetSocketAddress(InetAddress.getByName("localhost"), insecurePort);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    server.addEndpoint(new CoapEndpoint.Builder().setInetSocketAddress(addr).build());

    // add special interceptor for message traces
    for (Endpoint ep : server.getEndpoints()) {
      ep.addInterceptor(new MessageTracer());
    }
  }

  public static PrivateKey loadPrivateKey(String path) throws Exception {
    KeyFactory factory = KeyFactory.getInstance("RSA");
    PemReader pemReader = new PemReader(new InputStreamReader(new FileInputStream(path)));
    PKCS8EncodedKeySpec content = new PKCS8EncodedKeySpec(pemReader.readPemObject().getContent());
    return factory.generatePrivate(content);
  }

  private DTLSConnector getConnectorKeys(Integer port)  {
    DtlsConnectorConfig.Builder config = new DtlsConnectorConfig.Builder(Configuration.getStandard());
    InetSocketAddress addr = null;
    try {
      addr = new InetSocketAddress(InetAddress.getByName("localhost"), port);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    config.setAddress(addr);

    // load the trust store
    try {
      String PRIVATE_KEY = classLoader.getResource("keys/privatekey-pkcs8.pem").getPath();
      String PUBLIC_KEY = classLoader.getResource("keys/publickey.pem").getPath();
      AdvancedSinglePskStore psk = new AdvancedSinglePskStore("andrew","kebab".getBytes());
      config.setAdvancedPskStore(psk);

      return new DTLSConnector(config.build());
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new DTLSConnector(config.build());
  }


  private DTLSConnector getConnectorSSL(Integer port) {
    DtlsConnectorConfig.Builder config = new DtlsConnectorConfig.Builder(Configuration.getStandard());
    InetSocketAddress addr = null;
    try {
      addr = new InetSocketAddress(InetAddress.getByName("localhost"), port);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    config.setAddress(addr);
    // load the trust store
    try {
      KeyStore trustStore = KeyStore.getInstance("JKS");
      InputStream inTrust = Server.class.getClassLoader().getResourceAsStream(TRUST_STORE_LOCATION);
      trustStore.load(inTrust, TRUST_STORE_PASSWORD.toCharArray());

      // You can load multiple certificates if needed
      X509Certificate[] trustedCertificates = new X509Certificate[1];
      trustedCertificates[0] = (X509Certificate) trustStore.getCertificate("root");

      // load the key store
      KeyStore keyStore = KeyStore.getInstance("JKS");
      InputStream in = Server.class.getClassLoader().getResourceAsStream(KEY_STORE_LOCATION);
      keyStore.load(in, KEY_STORE_PASSWORD.toCharArray());

      PrivateKey privateKey = (PrivateKey)keyStore.getKey("server", KEY_STORE_PASSWORD.toCharArray());
      Certificate[] certChain = keyStore.getCertificateChain("server");
      config.setCertificateIdentityProvider(
              new SingleCertificateProvider(privateKey, certChain, CertificateType.RAW_PUBLIC_KEY, CertificateType.OPEN_PGP, CertificateType.X_509)
      );
      config.setCipherSuiteSelector(new DefaultCipherSuiteSelector());
      List<CertificateType> certificateTypeList = Lists.newArrayList(
              CertificateType.RAW_PUBLIC_KEY,
              CertificateType.OPEN_PGP,
              CertificateType.X_509
      );
      config.setAdvancedCertificateVerifier(
              StaticNewAdvancedCertificateVerifier
                      .builder()
                      .setTrustAllRPKs()
                      .build()
      );
      config.setAdvancedCertificateVerifier(
              new StaticNewAdvancedCertificateVerifier(
              trustedCertificates,
              null,
              certificateTypeList
      ));
      config.set(
              DtlsConfig.DTLS_CIPHER_SUITES,
              Lists.newArrayList((CipherSuite.getCipherSuites(false, false)))
      );
      return new DTLSConnector(config.build());
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new DTLSConnector(config.build());
  }


  public void start() {

    server.start();
    System.out.println("Secure CoAP server powered by Scandium (Sc) is listening on port " + secure );
    System.out.println("UnSecure CoAP server powered by Scandium (Sc) is listening on port " + insecure);
    System.out.println("Secure with PEM Keys CoAP server powered by Scandium (Sc) is listening on port " + key);

  }

  public void stop() {
    server.stop();
  }

  public static void main(String[] args) {
    Server server = new Server(5634, 5633, 5632);
    server.start();
  }

}