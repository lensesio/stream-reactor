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

/**
  * Created by andrew@datamountaineer.com on 04/01/2017. 
  * stream-reactor
  */
object CoapConstants {

  val COAP_KCQL = "connect.coap.kcql"
  val COAP_KCQL_DOC = "The KCQL statement to select and route resources to topics."

  val COAP_URI = "connect.coap.uri"
  val COAP_URI_DOC = "The COAP server to connect to."
  val COAP_URI_DEFAULT = "localhost"

  val COAP_DTLS_BIND_PORT = "connect.coap.bind.port"
  val COAP_DTLS_BIND_PORT_DEFAULT = 0
  val COAP_DTLS_BIND_PORT_DOC = "The port the DTLS connector will bind to on the Connector host."

  val COAP_DTLS_BIND_HOST = "connect.coap.bind.host"
  val COAP_DTLS_BIND_HOST_DEFAULT = "localhost"
  val COAP_DTLS_BIND_HOST_DOC = "The hostname the DTLS connector will bind to on the Connector host."

  //Security
  val COAP_TRUST_STORE_PASS = "connect.coap.truststore.pass"
  val COAP_TRUST_STORE_PASS_DOC = "The password of the trust store."
  val COAP_TRUST_STORE_PASS_DEFAULT = "rootPass"

  val COAP_TRUST_STORE_PATH = "connect.coap.truststore.path"
  val COAP_TRUST_STORE_PATH_DOC = "The path to the truststore."
  val COAP_TRUST_STORE_PATH_DEFAULT = ""

  val COAP_TRUST_CERTS = "connect.coap.certs"
  val COAP_TRUST_CERTS_DOC = "The certificates to load from the trust store."

  val COAP_KEY_STORE_PASS = "connect.coap.keystore.pass"
  val COAP_KEY_STORE_PASS_DOC = "The password of the key store."
  val COAP_KEY_STORE_PASS_DEFAULT = "rootPass"

  val COAP_KEY_STORE_PATH = "connect.coap.keystore.path"
  val COAP_KEY_STORE_PATH_DOC = "The path to the truststore."
  val COAP_KEY_STORE_PATH_DEFAULT = ""

  val COAP_CERT_CHAIN_KEY = "connect.coap.cert.chain.key"
  val COAP_CERT_CHAIN_KEY_DOC = "The key to use to get the certificate chain."
  val COAP_CERT_CHAIN_KEY_DEFAULT = "client"

  val ERROR_POLICY = "connect.coap.error.policy"
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

  val ERROR_RETRY_INTERVAL = "connect.coap.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES = "connect.coap.max.retries"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val COAP_DISCOVER_IP4 = "discoverIP4"
  val COAP_DISCOVER_IP6 = "discoverIP6"
  val COAP_DISCOVER_IP4_ADDRESS = "224.0.1.187"
  val COAP_DISCOVER_IP6_ADDRESS = "FF05::FD"

  val PROGRESS_COUNTER_ENABLED = "connect.progress.enabled"
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

  val BATCH_SIZE = "connect.coap.batch.size"
  val BATCH_SIZE_DEFAULT = 100
  val BATCH_SIZE_DOC = "The number of events to take from the internal queue to batch together to send to Kafka. The records will" +
    "be flushed if the linger period has expired first."

  val SOURCE_LINGER_MS = "connect.source.linger.ms"
  val SOURCE_LINGER_MS_DEFAULT = 5000
  val SOURCE_LINGER_MS_DOC = "The number of milliseconds to wait before flushing the received messages to Kafka. The records will" +
    "be flushed if the batch size is reached before the linger period has expired."
}
