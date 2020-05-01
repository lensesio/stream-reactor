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

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemReader;
import org.eclipse.californium.core.CaliforniumLogger;
import org.eclipse.californium.core.CoapServer;
import org.eclipse.californium.core.network.CoapEndpoint;
import org.eclipse.californium.core.network.Endpoint;
import org.eclipse.californium.core.network.config.NetworkConfig;
import org.eclipse.californium.core.network.interceptors.MessageTracer;
import org.eclipse.californium.scandium.DTLSConnector;
import org.eclipse.californium.scandium.ScandiumLogger;
import org.eclipse.californium.scandium.config.DtlsConnectorConfig;
import org.eclipse.californium.scandium.dtls.pskstore.InMemoryPskStore;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.*;
import java.security.cert.Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.logging.Level;

public class Server {

  static {
    CaliforniumLogger.initialize();
    CaliforniumLogger.setLevel(Level.CONFIG);
    ScandiumLogger.initialize();
    ScandiumLogger.setLevel(Level.FINER);
  }

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
    Security.addProvider(new BouncyCastleProvider());
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
    server.addEndpoint(new CoapEndpoint(sslConnector, NetworkConfig.getStandard()));

    //add key
    server.addEndpoint(new CoapEndpoint(keyConnector, NetworkConfig.getStandard()));

    //add unsecure
    InetSocketAddress addr = null;
    try {
      addr = new InetSocketAddress(InetAddress.getByName("localhost"), insecurePort);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    server.addEndpoint(new CoapEndpoint(addr));

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

  public static PublicKey loadPublicKey(String path) throws Exception {
    KeyFactory factory = KeyFactory.getInstance("RSA");
    PemReader pemReader = new PemReader(new InputStreamReader(new FileInputStream(path)));
    X509EncodedKeySpec content = new X509EncodedKeySpec(pemReader.readPemObject().getContent());
    return factory.generatePublic(content);
  }

  private DTLSConnector getConnectorKeys(Integer port)  {
    DtlsConnectorConfig.Builder config = new DtlsConnectorConfig.Builder();
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
     // config.setIdentity(loadPrivateKey(PRIVATE_KEY), loadPublicKey(PUBLIC_KEY));
     // config.setSupportedCipherSuites(new CipherSuite[]{CipherSuite.TLS_PSK_WITH_AES_128_CCM_8, CipherSuite.TLS_PSK_WITH_AES_128_CBC_SHA256});
      InMemoryPskStore psk = new InMemoryPskStore();
      psk.setKey("andrew","kebab".getBytes());
      psk.addKnownPeer(addr, "andrew","kebab".getBytes());
      config.setPskStore(psk);

      return new DTLSConnector(config.build());
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new DTLSConnector(config.build());
  }


  private DTLSConnector getConnectorSSL(Integer port) {
    DtlsConnectorConfig.Builder config = new DtlsConnectorConfig.Builder();
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
      Certificate[] trustedCertificates = new Certificate[1];
      trustedCertificates[0] = trustStore.getCertificate("root");

      // load the key store
      KeyStore keyStore = KeyStore.getInstance("JKS");
      InputStream in = Server.class.getClassLoader().getResourceAsStream(KEY_STORE_LOCATION);
      keyStore.load(in, KEY_STORE_PASSWORD.toCharArray());

      config.setIdentity((PrivateKey)keyStore.getKey("server", KEY_STORE_PASSWORD.toCharArray()),
          keyStore.getCertificateChain("server"), true);

      config.setTrustStore(trustedCertificates);
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