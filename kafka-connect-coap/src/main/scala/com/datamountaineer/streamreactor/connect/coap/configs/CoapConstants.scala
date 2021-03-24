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

package com.datamountaineer.streamreactor.connect.coap.configs

import com.datamountaineer.streamreactor.common.config.base.const.TraitConfigConst._

/**
  * Created by andrew@datamountaineer.com on 04/01/2017. 
  * stream-reactor
  */
object CoapConstants {
  val CONNECTOR_PREFIX = s"connect.coap"

  val COAP_KCQL = s"${CONNECTOR_PREFIX}.${KCQL_PROP_SUFFIX}"
  val COAP_KCQL_DOC = "The KCQL statement to select and route resources to topics."

  val COAP_URI = s"${CONNECTOR_PREFIX}.${URI_SUFFIX}"
  val COAP_URI_DOC = "The COAP server to connect to."
  val COAP_URI_DEFAULT = "localhost"

  val COAP_DTLS_BIND_PORT = s"${CONNECTOR_PREFIX}.${CONNECTION_PORT_SUFFIX}"
  val COAP_DTLS_BIND_PORT_DEFAULT = 0
  val COAP_DTLS_BIND_PORT_DOC = "The port the DTLS connector will bind to on the Connector host."

  val COAP_DTLS_BIND_HOST = s"${CONNECTOR_PREFIX}.${CONNECTION_HOST_SUFFIX}"
  val COAP_DTLS_BIND_HOST_DEFAULT = "localhost"
  val COAP_DTLS_BIND_HOST_DOC = "The hostname the DTLS connector will bind to on the Connector host."

  //Security

  //PSK
  val COAP_IDENTITY = s"$CONNECTOR_PREFIX.$USERNAME_SUFFIX"
  val COAP_IDENTITY_DEFAULT = ""
  val COAP_IDENTITY_DOC = "CoAP PSK identity."

  val COAP_SECRET = s"$CONNECTOR_PREFIX.$PASSWORD_SUFFIX"
  val COAP_SECRET_DEFAULT = ""
  val COAP_SECRET_DOC = "CoAP PSK secret."

  //PUBLIC/PRIVATE KEYS
  val COAP_PUBLIC_KEY_FILE = s"$CONNECTOR_PREFIX.public.key.file"
  val COAP_PUBLIC_KEY_FILE_DEFAULT = ""
  val COAP_PUBLIC_KEY_FILE_DOC = "Path to the public key file for use in with PSK credentials"

  val COAP_PRIVATE_KEY_FILE = s"$CONNECTOR_PREFIX.private.key.file"
  val COAP_PRIVATE_KEY_FILE_DEFAULT = ""
  val COAP_PRIVATE_KEY_FILE_DOC =
    """
      | Path to the private key file for use in with PSK credentials in PKCS8 rather than PKCS1
      | Use open SSL to convert.
      |
      | `openssl pkcs8 -in privatekey.pem -topk8 -nocrypt -out privatekey-pkcs8.pem`
      |
      | Only cipher suites TLS_PSK_WITH_AES_128_CCM_8 and TLS_PSK_WITH_AES_128_CBC_SHA256 are currently supported.
    """.stripMargin

  val COAP_TRUST_STORE_PASS = s"${CONNECTOR_PREFIX}.${TRUSTSTORE_PASS_SUFFIX}"
  val COAP_TRUST_STORE_PASS_DOC = "The password of the trust store."
  val COAP_TRUST_STORE_PASS_DEFAULT = "rootPass"

  val COAP_TRUST_STORE_PATH = s"${CONNECTOR_PREFIX}.${TRUSTSTORE_PATH_SUFFIX}"
  val COAP_TRUST_STORE_PATH_DOC = "The path to the truststore."
  val COAP_TRUST_STORE_PATH_DEFAULT = ""

  val COAP_TRUST_CERTS = s"${CONNECTOR_PREFIX}.${CERTIFICATES_SUFFIX}"
  val COAP_TRUST_CERTS_DOC = "The certificates to load from the trust store."

  val COAP_KEY_STORE_PASS = s"${CONNECTOR_PREFIX}.${KEYSTORE_PASS_SUFFIX}"
  val COAP_KEY_STORE_PASS_DOC = "The password of the key store."
  val COAP_KEY_STORE_PASS_DEFAULT = "rootPass"

  val COAP_KEY_STORE_PATH = s"${CONNECTOR_PREFIX}.${KEYSTORE_PATH_SUFFIX}"
  val COAP_KEY_STORE_PATH_DOC = "The path to the truststore."
  val COAP_KEY_STORE_PATH_DEFAULT = ""

  val COAP_CERT_CHAIN_KEY = s"${CONNECTOR_PREFIX}.${CERTIFICATE_KEY_CHAIN_SUFFIX}"
  val COAP_CERT_CHAIN_KEY_DOC = "The key to use to get the certificate chain."
  val COAP_CERT_CHAIN_KEY_DEFAULT = "client"

  val COAP_DISCOVER_IP4 = "discoverIP4"
  val COAP_DISCOVER_IP6 = "discoverIP6"
  val COAP_DISCOVER_IP4_ADDRESS = "224.0.1.187"
  val COAP_DISCOVER_IP6_ADDRESS = "FF05::FD"

  val ERROR_POLICY = s"${CONNECTOR_PREFIX}.${ERROR_POLICY_PROP_SUFFIX}"
  val ERROR_POLICY_DOC: String =
    """
      |Specifies the action to be taken if an error occurs while inserting the data.
      | There are three available options:
      |    NOOP - the error is swallowed
      |    THROW - the error is allowed to propagate.
      |    RETRY - The exception causes the Connect framework to retry the message. The number of retries is set by connect.cassandra.max.retries.
      |All errors will be logged automatically, even if the code swallows them.
    """.stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = s"${CONNECTOR_PREFIX}.${RETRY_INTERVAL_PROP_SUFFIX}"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"

  val NBR_OF_RETRIES = s"${CONNECTOR_PREFIX}.${MAX_RETRIES_PROP_SUFFIX}"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20


  val PROGRESS_COUNTER_ENABLED = s"${PROGRESS_ENABLED_CONST}"
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

  val BATCH_SIZE = s"${CONNECTOR_PREFIX}.${BATCH_SIZE_PROP_SUFFIX}"
  val BATCH_SIZE_DEFAULT = 100
  val BATCH_SIZE_DOC = "The number of events to take from the internal queue to batch together to send to Kafka. The records will" +
    "be flushed if the linger period has expired first."

  val SOURCE_LINGER_MS = "connect.source.linger.ms"
  val SOURCE_LINGER_MS_DEFAULT = 5000
  val SOURCE_LINGER_MS_DOC = "The number of milliseconds to wait before flushing the received messages to Kafka. The records will" +
    "be flushed if the batch size is reached before the linger period has expired."
}
