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
package io.lenses.streamreactor.connect.cassandra

import io.lenses.streamreactor.common.config.base.const.TraitConfigConst._

/**
 * Created by andrew@datamountaineer.com on 25/04/16.
 * stream-reactor
 */

/**
 * Holds the constants used in the config.
 */

object CassandraConfigConstants {
  val CONNECTOR_NAME_KEY = "name"
  val CONNECTOR_PREFIX   = "connect.cassandra"

  val NONE = "none"

  val CONTACT_POINTS        = s"$CONNECTOR_PREFIX.contact.points"
  val CONTACT_POINT_DOC     = "A comma-separated list of host names or IP addresses."
  val CONTACT_POINT_DEFAULT = "localhost"

  val PORT         = s"$CONNECTOR_PREFIX.port"
  val PORT_DEFAULT = "9042"
  val PORT_DOC     = "Cassandra native port."

  val MAX_CONCURRENT_REQUESTS         = s"$CONNECTOR_PREFIX.max.concurrent.requests"
  val MAX_CONCURRENT_REQUESTS_DOC     = "Maximum number of concurrent requests sent to the database."
  val MAX_CONCURRENT_REQUESTS_DEFAULT = "100"

  val CONNECTION_POOL_SIZE = s"$CONNECTOR_PREFIX.connection.pool.size"
  val CONNECTION_POOL_SIZE_DOC =
    "Number of connections that driver maintains within a connection pool to each node in the local datacenter."
  val CONNECTION_POOL_SIZE_DEFAULT = "2"

  val COMPRESSION         = s"$CONNECTOR_PREFIX.compression"
  val COMPRESSION_DOC     = "Compression algorithm to use for the connection. Defaults to None."
  val COMPRESSION_DEFAULT = "none"

  val QUERY_TIMEOUT         = s"$CONNECTOR_PREFIX.query.timeout.ms"
  val QUERY_TIMEOUT_DOC     = "The Cassandra driver query timeout in milliseconds."
  val QUERY_TIMEOUT_DEFAULT = 20000

  val MAX_BATCH_SIZE         = s"$CONNECTOR_PREFIX.max.batch.size"
  val MAX_BATCH_SIZE_DOC     = "Number of records to include in a write request to the database table."
  val MAX_BATCH_SIZE_DEFAULT = 64

  val LOAD_BALANCING_LOCAL_DC = s"$CONNECTOR_PREFIX.load.balancing.local.dc"
  val LOAD_BALANCING_LOCAL_DC_DOC =
    "The local data center to use for load balancing. If not set, the driver will use the first data center in the list of available data centers."

  val AUTH_PROVIDER         = s"$CONNECTOR_PREFIX.auth.provider"
  val AUTH_PROVIDER_DOC     = "Authentication provider"
  val AUTH_PROVIDER_DEFAULT = "None"

  val AUTH_USERNAME         = s"$CONNECTOR_PREFIX.auth.username"
  val AUTH_USERNAME_DOC     = "Username for PLAIN (username/password) provider authentication"
  val AUTH_USERNAME_DEFAULT = ""

  val AUTH_PASSWORD         = s"$CONNECTOR_PREFIX.auth.password"
  val AUTH_PASSWORD_DOC     = "Password for PLAIN (username/password) provider authentication"
  val AUTH_PASSWORD_DEFAULT = ""

  val AUTH_GSSAPI_KEYTAB         = s"$CONNECTOR_PREFIX.auth.gssapi.keytab"
  val AUTH_GSSAPI_KEYTAB_DOC     = "Kerberos keytab file for GSSAPI provider authentication"
  val AUTH_GSSAPI_KEYTAB_DEFAULT = ""

  val AUTH_GSSAPI_PRINCIPAL         = s"$CONNECTOR_PREFIX.auth.gssapi.principal"
  val AUTH_GSSAPI_PRINCIPAL_DOC     = "Kerberos principal for GSSAPI provider authentication"
  val AUTH_GSSAPI_PRINCIPAL_DEFAULT = ""

  val AUTH_GSSAPI_SERVICE         = s"$CONNECTOR_PREFIX.auth.gssapi.service"
  val AUTH_GSSAPI_SERVICE_DOC     = "SASL service name to use for GSSAPI provider authentication"
  val AUTH_GSSAPI_SERVICE_DEFAULT = "dse"

  val SSL_ENABLED         = s"$CONNECTOR_PREFIX.ssl.enabled"
  val SSL_ENABLED_DOC     = "Secure Cassandra driver connection via SSL."
  val SSL_ENABLED_DEFAULT = "false"

  val SSL_PREFIX           = s"$CONNECTOR_PREFIX.ssl"
  val SSL_PROVIDER         = s"$SSL_PREFIX.provider"
  val SSL_PROVIDER_DEFAULT = "None"
  val SSL_PROVIDER_DOC =
    "The SSL provider to use for the connection. Available values are None, JDK or OpenSSL. Defaults to None."

  val SSL_TRUST_STORE_PATH       = s"$SSL_PREFIX.truststore.path"
  val SSL_TRUST_STORE_PATH_DOC   = "Path to the client Trust Store."
  val SSL_TRUST_STORE_PASSWD     = s"$SSL_PREFIX.truststore.password"
  val SSL_TRUST_STORE_PASSWD_DOC = "Password for the client Trust Store."

  val SSL_KEY_STORE_PATH       = s"$SSL_PREFIX.keystore.path"
  val SSL_KEY_STORE_PATH_DOC   = "Path to the client Key Store."
  val SSL_KEY_STORE_PASSWD     = s"$SSL_PREFIX.keystore.password"
  val SSL_KEY_STORE_PASSWD_DOC = "Password for the client Key Store"

  val SSL_CIPHER_SUITES     = s"$SSL_PREFIX.cipher.suites"
  val SSL_CIPHER_SUITES_DOC = "The SSL cipher suites to use for the connection."

  val SSL_HOSTNAME_VERIFICATION         = s"$SSL_PREFIX.hostname.verification"
  val SSL_HOSTNAME_VERIFICATION_DEFAULT = "true"
  val SSL_HOSTNAME_VERIFICATION_DOC     = "Enable hostname verification for the connection."

  val SSL_OPENSSL_KEY_CERT_CHAIN     = s"$SSL_PREFIX.openssl.key.cert.chain"
  val SSL_OPENSSL_KEY_CERT_CHAIN_DOC = "Enable OpenSSL key certificate chain for the connection."

  val SSL_OPENSSL_PRIVATE_KEY     = s"$SSL_PREFIX.openssl.private.key"
  val SSL_OPENSSL_PRIVATE_KEY_DOC = "Enable OpenSSL private key for the connection."

  val IGNORE_ERRORS_MODE         = s"$CONNECTOR_PREFIX.ignore.errors.mode"
  val IGNORE_ERRORS_MODE_DOC     = "Can be one of 'none', 'all' or 'driver'"
  val IGNORE_ERRORS_MODE_DEFAULT = "none"

  val ERROR_RETRY_INTERVAL         = s"$CONNECTOR_PREFIX.$RETRY_INTERVAL_PROP_SUFFIX"
  val ERROR_RETRY_INTERVAL_DOC     = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"

  val ERROR_POLICY = s"$CONNECTOR_PREFIX.$ERROR_POLICY_PROP_SUFFIX"
  val ERROR_POLICY_DOC =
    """
      |Specifies the action to be taken if an error occurs while inserting the data.
      | There are three available options:
      |    NOOP - the error is swallowed
      |    THROW - the error is allowed to propagate.
      |    RETRY - The exception causes the Connect framework to retry the message. The number of retries is set by connect.cassandra.max.retries.
      |All errors will be logged automatically, even if the code swallows them.
    """.stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val NBR_OF_RETRIES         = s"$CONNECTOR_PREFIX.$MAX_RETRIES_PROP_SUFFIX"
  val NBR_OF_RETRIES_DOC     = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val KCQL     = s"$CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"
  val KCQL_DOC = "KCQL expression describing field selection and routes."

  val PROGRESS_COUNTER_ENABLED: String = PROGRESS_ENABLED_CONST
  val PROGRESS_COUNTER_ENABLED_DOC     = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

}
