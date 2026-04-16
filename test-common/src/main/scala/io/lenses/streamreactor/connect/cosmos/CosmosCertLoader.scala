/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.cosmos

import com.typesafe.scalalogging.LazyLogging
import org.testcontainers.containers.GenericContainer

import java.io._
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import scala.util.Try

object CosmosCertLoader extends LazyLogging {
  @throws[Exception]
  def extractCertAsTrustStore(container: GenericContainer[_]): KeyStore = {
    // Step 1: Copy PEM from container to local temp file
    val tempPemFile = File.createTempFile("cosmos-emulator-cert", ".pem")
    tempPemFile.deleteOnExit()
    // ca-bundle.crt -> /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem
    // ca-bundle.trust.crt -> /etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt
    // ca-certificates.crt -> /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem
    // orbstack-root.crt
    val certs = Seq(
      "/scripts/certs/domain.crt",
      "/scripts/certs/rootCA.crt",
    )
    val cf         = CertificateFactory.getInstance("X.509")
    val trustStore = KeyStore.getInstance(KeyStore.getDefaultType)
    trustStore.load(null, null) // initialize empty

    certs.zipWithIndex.foreach {
      case (crt, idx) =>
        Try(container.copyFileFromContainer(
          crt,
          tempPemFile.getAbsolutePath,
        ))

        // Step 2: Load cert into KeyStore
        val certInputStream = new FileInputStream(tempPemFile)
        try {
          val cert = cf.generateCertificate(certInputStream).asInstanceOf[X509Certificate]
          trustStore.setCertificateEntry(s"cosmos-emulator-$idx", cert)

        } catch {
          case t: Throwable => logger.error("error generating cert loading keysstore", t)
        } finally if (certInputStream != null) certInputStream.close()

    }
    trustStore
  }
}
