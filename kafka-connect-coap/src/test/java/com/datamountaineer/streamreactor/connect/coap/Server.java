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

import org.eclipse.californium.core.CaliforniumLogger;
import org.eclipse.californium.core.CoapServer;
import org.eclipse.californium.core.network.CoapEndpoint;
import org.eclipse.californium.core.network.Endpoint;
import org.eclipse.californium.core.network.config.NetworkConfig;
import org.eclipse.californium.core.network.interceptors.MessageTracer;
import org.eclipse.californium.scandium.DTLSConnector;
import org.eclipse.californium.scandium.ScandiumLogger;
import org.eclipse.californium.scandium.config.DtlsConnectorConfig;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.logging.Level;

public class Server {

  static {
    CaliforniumLogger.initialize();
    CaliforniumLogger.setLevel(Level.CONFIG);
    ScandiumLogger.initialize();
    ScandiumLogger.setLevel(Level.FINER);
  }

  private CoapServer server;

  private static final String TRUST_STORE_PASSWORD = "rootPass";
  private final static String KEY_STORE_PASSWORD = "endPass";
  private static final String KEY_STORE_LOCATION = "certs2/keyStore.jks";
  private static final String TRUST_STORE_LOCATION = "certs2/trustStore.jks";
  Integer secure;
  Integer insecure;

  public Server(Integer securePort, Integer insecurePort) {

    server = new CoapServer();

//    InetAddress addr = null;
//    try {
//      addr = InetAddress.getByName("224.0.1.187");
//    } catch (UnknownHostException e) {
//      e.printStackTrace();
//    }
//    InetSocketAddress bindToAddressSecure = new InetSocketAddress(addr, securePort);
//    CoapEndpoint multicastSecure = new CoapEndpoint(bindToAddressSecure);
//
//    InetSocketAddress bindToAddressInSecure = new InetSocketAddress(addr, insecurePort);
//    CoapEndpoint multicastInSecure = new CoapEndpoint(bindToAddressInSecure);

    server.add(new ObservableResource("secure"));
    server.add(new ObservableResource("insecure"));

//    server.addEndpoint(multicastInSecure);
//    server.addEndpoint(multicastSecure);

    secure = securePort;
    insecure = insecurePort;

    try {
      // Pre-shared secrets
      //InMemoryPskStore pskStore = new InMemoryPskStore();
      //pskStore.setKey("Client_identity", "secretPSK".getBytes()); // from ETSI Plugtest test spec

      // load the trust store
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

      DtlsConnectorConfig.Builder config = new DtlsConnectorConfig.Builder(new InetSocketAddress(securePort));
      //config.setSupportedCipherSuites(new CipherSuite[]{CipherSuite.TLS_PSK_WITH_AES_128_CCM_8,
      //    CipherSuite.TLS_ECDHE_ECDSA_WITH_AES_128_CCM_8});
      //config.setPskStore(pskStore);
      config.setIdentity((PrivateKey)keyStore.getKey("server", KEY_STORE_PASSWORD.toCharArray()),
          keyStore.getCertificateChain("server"), true);
      config.setTrustStore(trustedCertificates);

      DTLSConnector connector = new DTLSConnector(config.build());

      //add secure
      server.addEndpoint(new CoapEndpoint(connector, NetworkConfig.getStandard()));
      //add unsecure

      InetSocketAddress addr = new InetSocketAddress(InetAddress.getByName("localhost"), insecurePort);
      server.addEndpoint(new CoapEndpoint(addr));

    } catch (GeneralSecurityException | IOException e) {
      System.err.println("Could not load the keystore");
      e.printStackTrace();
    }

    // add special interceptor for message traces
    for (Endpoint ep : server.getEndpoints()) {
      ep.addInterceptor(new MessageTracer());
    }
  }


  public void start() {

    server.start();
    System.out.println("Secure CoAP server powered by Scandium (Sc) is listening on port " + secure );
    System.out.println("UnSecure CoAP server powered by Scandium (Sc) is listening on port " + insecure);

  }

  public void stop() {
    server.stop();
  }

  public static void main(String[] args) {
    Server server = new Server(5634, 5633);
    server.start();
  }


}