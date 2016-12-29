package com.datamountaineer.streamreactor.connect.coap;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.logging.Level;

import org.eclipse.californium.core.CaliforniumLogger;
import org.eclipse.californium.core.CoapServer;
import org.eclipse.californium.core.network.CoapEndpoint;
import org.eclipse.californium.core.network.Endpoint;
import org.eclipse.californium.core.network.config.NetworkConfig;
import org.eclipse.californium.core.network.interceptors.MessageTracer;
import org.eclipse.californium.scandium.DTLSConnector;
import org.eclipse.californium.scandium.ScandiumLogger;
import org.eclipse.californium.scandium.config.DtlsConnectorConfig;


public class Server {

  static {
    CaliforniumLogger.initialize();
    CaliforniumLogger.setLevel(Level.CONFIG);
    ScandiumLogger.initialize();
    ScandiumLogger.setLevel(Level.FINER);
  }

  // allows configuration via Californium.properties
  public static final int DTLS_PORT = NetworkConfig.getStandard().getInt(NetworkConfig.Keys.COAP_SECURE_PORT);
  public static final int PORT = NetworkConfig.getStandard().getInt(NetworkConfig.Keys.COAP_PORT);
  public static final String SECURE_PAYLOAD = "hello security";
  public static final String INSECURE_PAYLOAD = "hello no security";

  private CoapServer server;

  private static final String TRUST_STORE_PASSWORD = "rootPass";
  private final static String KEY_STORE_PASSWORD = "endPass";
  private static final String KEY_STORE_LOCATION = "certs2/keyStore.jks";
  private static final String TRUST_STORE_LOCATION = "certs2/trustStore.jks";

  public Server() {

    server = new CoapServer();
    server.add(new ObservableResource("secure"));
    server.add(new ObservableResource("insecure"));

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

      DtlsConnectorConfig.Builder config = new DtlsConnectorConfig.Builder(new InetSocketAddress(DTLS_PORT));
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
      server.addEndpoint(new CoapEndpoint(new InetSocketAddress("localhost", PORT)));

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
    System.out.println("Secure CoAP server powered by Scandium (Sc) is listening on port " + DTLS_PORT);
    System.out.println("UnSecure CoAP server powered by Scandium (Sc) is listening on port " + PORT);
  }

  public void stop() {
    server.stop();
  }

  public static void main(String[] args) {
    Server server = new Server();
    server.start();
  }

}