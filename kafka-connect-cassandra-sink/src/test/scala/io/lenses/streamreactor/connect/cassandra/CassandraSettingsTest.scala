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

import org.apache.kafka.common.config.ConfigException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CassandraSettingsTest extends AnyFunSuite with Matchers {

  test("creates CassandraSettings with minimal valid configuration") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "None",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.connectorName shouldBe "test-connector"
    settings.contactPoints shouldBe Seq("localhost:9042")
    settings.port shouldBe 9042
    settings.maxConcurrentRequests shouldBe 100
    settings.connectionPoolSize shouldBe 2
    settings.compression shouldBe "NONE"
    settings.queryTimeout shouldBe 20000
    settings.maxBatchSize shouldBe 64
    settings.auth shouldBe None
    settings.ssl shouldBe None
    settings.ignoredError shouldBe IgnoredErrorMode.None
    settings.taskRetries shouldBe 20
    settings.enableProgress shouldBe false
    settings.tableSettings should have size 1
    settings.tableSettings.head.namespace shouldBe "testks"
    settings.tableSettings.head.table shouldBe "testtable"
    settings.tableSettings.head.sourceKafkaTopic shouldBe "testtopic"
  }

  test("creates CassandraSettings with SSL configuration") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY        -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS            -> "localhost:9042",
        CassandraConfigConstants.PORT                      -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS   -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE      -> "2",
        CassandraConfigConstants.COMPRESSION               -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT             -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE            -> "64",
        CassandraConfigConstants.AUTH_PROVIDER             -> "None",
        CassandraConfigConstants.SSL_ENABLED               -> "true",
        CassandraConfigConstants.SSL_PROVIDER              -> "JDK",
        CassandraConfigConstants.SSL_TRUST_STORE_PATH      -> "/path/to/truststore",
        CassandraConfigConstants.SSL_TRUST_STORE_PASSWD    -> "trustpass",
        CassandraConfigConstants.SSL_KEY_STORE_PATH        -> "/path/to/keystore",
        CassandraConfigConstants.SSL_KEY_STORE_PASSWD      -> "keypass",
        CassandraConfigConstants.SSL_HOSTNAME_VERIFICATION -> "true",
        CassandraConfigConstants.IGNORE_ERRORS_MODE        -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES            -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL      -> "60000",
        CassandraConfigConstants.KCQL                      -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED  -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.ssl shouldBe defined
    settings.ssl.get.provider shouldBe Some("JDK")
    settings.ssl.get.trustStorePath shouldBe Some("/path/to/truststore")
    settings.ssl.get.trustStorePass shouldBe Some("trustpass")
    settings.ssl.get.keyStorePath shouldBe Some("/path/to/keystore")
    settings.ssl.get.keyStorePass shouldBe Some("keypass")
    settings.ssl.get.hostnameVerification shouldBe true
  }

  test("creates CassandraSettings with PLAIN authentication") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "PLAIN",
        CassandraConfigConstants.AUTH_USERNAME            -> "testuser",
        CassandraConfigConstants.AUTH_PASSWORD            -> "testpass",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.auth shouldBe defined
    settings.auth.get.provider shouldBe "PLAIN"
    settings.auth.get.username shouldBe Some("testuser")
    settings.auth.get.password shouldBe Some("testpass")
  }

  test("creates CassandraSettings with GSSAPI authentication") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "GSSAPI",
        CassandraConfigConstants.AUTH_GSSAPI_KEYTAB       -> "/path/to/keytab",
        CassandraConfigConstants.AUTH_GSSAPI_PRINCIPAL    -> "test@REALM",
        CassandraConfigConstants.AUTH_GSSAPI_SERVICE      -> "cassandra",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.auth shouldBe defined
    settings.auth.get.provider shouldBe "GSSAPI"
    settings.auth.get.gssapiKeyTab shouldBe Some("/path/to/keytab")
    settings.auth.get.gssapiPrincipal shouldBe Some("test@REALM")
    settings.auth.get.gssapiService shouldBe "cassandra"
  }

  test("creates CassandraSettings with multiple KCQL statements") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "None",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.table1 SELECT * FROM topic1; INSERT INTO testks.table2 SELECT * FROM topic2",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.tableSettings should have size 2
    settings.tableSettings(0).namespace shouldBe "testks"
    settings.tableSettings(0).table shouldBe "table1"
    settings.tableSettings(0).sourceKafkaTopic shouldBe "topic1"
    settings.tableSettings(1).namespace shouldBe "testks"
    settings.tableSettings(1).table shouldBe "table2"
    settings.tableSettings(1).sourceKafkaTopic shouldBe "topic2"
  }

  test("creates CassandraSettings with different error policies") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "None",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "all",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.ignoredError shouldBe IgnoredErrorMode.All
  }

  test("creates CassandraSettings with local DC configuration") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "None",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.LOAD_BALANCING_LOCAL_DC  -> "datacenter1",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.localDc shouldBe Some("datacenter1")
  }

  test("creates CassandraSettings with driver settings") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "None",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
        "connect.cassandra.driver.custom.setting"         -> "custom-value",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.driverSettings should contain("datastax-java-driver.custom.setting" -> "custom-value")
  }

  test("throws ConfigException when contact points are empty") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "None",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    a[ConfigException] should be thrownBy CassandraSettings.configureSink(config)
  }

  test("throws ConfigException when contact points contain only whitespace") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "   ,  ,  ",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "None",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    a[ConfigException] should be thrownBy CassandraSettings.configureSink(config)
  }

  test("parses multiple contact points correctly") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "host1:9042, host2:9043 , host3:9044",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "None",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.contactPoints shouldBe Seq("host1:9042", "host2:9043", "host3:9044")
  }

  test("handles empty optional fields correctly") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "PLAIN",
        CassandraConfigConstants.AUTH_USERNAME            -> "",
        CassandraConfigConstants.AUTH_PASSWORD            -> "",
        CassandraConfigConstants.SSL_ENABLED              -> "true",
        CassandraConfigConstants.SSL_PROVIDER             -> "NONE",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.auth shouldBe defined
    settings.auth.get.username shouldBe None
    settings.auth.get.password shouldBe None
    settings.ssl shouldBe defined
    settings.ssl.get.provider shouldBe Some("NONE")
  }

  test("creates CassandraSettings with complete SSL configuration") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY         -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS             -> "localhost:9042",
        CassandraConfigConstants.PORT                       -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS    -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE       -> "2",
        CassandraConfigConstants.COMPRESSION                -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT              -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE             -> "64",
        CassandraConfigConstants.AUTH_PROVIDER              -> "None",
        CassandraConfigConstants.SSL_ENABLED                -> "true",
        CassandraConfigConstants.SSL_PROVIDER               -> "OpenSSL",
        CassandraConfigConstants.SSL_TRUST_STORE_PATH       -> "/path/to/truststore.jks",
        CassandraConfigConstants.SSL_TRUST_STORE_PASSWD     -> "trustpass123",
        CassandraConfigConstants.SSL_KEY_STORE_PATH         -> "/path/to/keystore.jks",
        CassandraConfigConstants.SSL_KEY_STORE_PASSWD       -> "keypass123",
        CassandraConfigConstants.SSL_CIPHER_SUITES          -> "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA",
        CassandraConfigConstants.SSL_HOSTNAME_VERIFICATION  -> "false",
        CassandraConfigConstants.SSL_OPENSSL_KEY_CERT_CHAIN -> "/path/to/cert.pem",
        CassandraConfigConstants.SSL_OPENSSL_PRIVATE_KEY    -> "/path/to/key.pem",
        CassandraConfigConstants.IGNORE_ERRORS_MODE         -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES             -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL       -> "60000",
        CassandraConfigConstants.KCQL                       -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED   -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.ssl shouldBe defined
    val ssl = settings.ssl.get
    ssl.provider shouldBe Some("OpenSSL")
    ssl.trustStorePath shouldBe Some("/path/to/truststore.jks")
    ssl.trustStorePass shouldBe Some("trustpass123")
    ssl.keyStorePath shouldBe Some("/path/to/keystore.jks")
    ssl.keyStorePass shouldBe Some("keypass123")
    ssl.cipherSuites shouldBe Some("TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA")
    ssl.hostnameVerification shouldBe false
    ssl.openSslKeyCertChain shouldBe Some("/path/to/cert.pem")
    ssl.openSslPrivateKey shouldBe Some("/path/to/key.pem")
  }

  test("creates CassandraSettings with complete PLAIN authentication") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "PLAIN",
        CassandraConfigConstants.AUTH_USERNAME            -> "cassandra_user",
        CassandraConfigConstants.AUTH_PASSWORD            -> "secure_password_123",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.auth shouldBe defined
    val auth = settings.auth.get
    auth.provider shouldBe "PLAIN"
    auth.username shouldBe Some("cassandra_user")
    auth.password shouldBe Some("secure_password_123")
    auth.gssapiKeyTab shouldBe None
    auth.gssapiPrincipal shouldBe None
    auth.gssapiService shouldBe "dse" // default value
  }

  test("creates CassandraSettings with complete GSSAPI authentication") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "GSSAPI",
        CassandraConfigConstants.AUTH_GSSAPI_KEYTAB       -> "/etc/security/keytabs/cassandra.keytab",
        CassandraConfigConstants.AUTH_GSSAPI_PRINCIPAL    -> "cassandra/cassandra.example.com@EXAMPLE.COM",
        CassandraConfigConstants.AUTH_GSSAPI_SERVICE      -> "cassandra",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.auth shouldBe defined
    val auth = settings.auth.get
    auth.provider shouldBe "GSSAPI"
    auth.username shouldBe None
    auth.password shouldBe None
    auth.gssapiKeyTab shouldBe Some("/etc/security/keytabs/cassandra.keytab")
    auth.gssapiPrincipal shouldBe Some("cassandra/cassandra.example.com@EXAMPLE.COM")
    auth.gssapiService shouldBe "cassandra"
  }

  test("creates CassandraTableSetting with field mappings and properties") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "None",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> """INSERT INTO testks.testtable 
                                           |SELECT 
                                           |  _key.id as key_id,
                                           |  _header.trace_id as trace_id,
                                           |  name as user_name,
                                           |  age as user_age
                                           |FROM testtopic 
                                           |PROPERTIES('behavior.on.null.values'='IGNORE', 'consistency.level'='ONE')""".stripMargin,
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.tableSettings should have size 1
    val tableSetting = settings.tableSettings.head
    tableSetting.namespace shouldBe "testks"
    tableSetting.table shouldBe "testtable"
    tableSetting.sourceKafkaTopic shouldBe "testtopic"

    tableSetting.fieldMappings should have size 4
    tableSetting.fieldMappings.find(_.to == "key_id").get.fieldType shouldBe "key"
    tableSetting.fieldMappings.find(_.to == "trace_id").get.fieldType shouldBe "header"
    tableSetting.fieldMappings.find(_.to == "user_name").get.fieldType shouldBe "value"
    tableSetting.fieldMappings.find(_.to == "user_age").get.fieldType shouldBe "value"

    tableSetting.others should contain("behavior.on.null.values" -> "IGNORE")
    tableSetting.others should contain("consistency.level" -> "ONE")
  }

  test("creates CassandraTableSetting with wildcard field mapping") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "None",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.tableSettings should have size 1
    val tableSetting = settings.tableSettings.head
    tableSetting.fieldMappings shouldBe empty // wildcard fields are filtered out
  }

  test("validates Cassandra table and namespace names") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "None",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO TestKeyspace.TestTable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.tableSettings should have size 1
    val tableSetting = settings.tableSettings.head
    tableSetting.namespace shouldBe "TestKeyspace"
    tableSetting.table shouldBe "TestTable"
  }

  test("throws IllegalArgumentException for invalid table/namespace names") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "None",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO 1invalid.table SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    an[ConfigException] should be thrownBy CassandraSettings.configureSink(config)
  }

  test("throws IllegalArgumentException for KCQL without namespace.table format") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "None",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "none",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO invalidtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val ex = intercept[ConfigException] {
      CassandraSettings.configureSink(config)
    }
    ex.getMessage shouldBe "KCQL target must be in the form namespace.table, got: invalidtable"
  }

  test("creates CassandraSettings with driver error mode") {
    val config = CassandraConfig(
      Map(
        CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
        CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
        CassandraConfigConstants.PORT                     -> "9042",
        CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
        CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
        CassandraConfigConstants.COMPRESSION              -> "NONE",
        CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
        CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
        CassandraConfigConstants.AUTH_PROVIDER            -> "None",
        CassandraConfigConstants.SSL_ENABLED              -> "false",
        CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "driver",
        CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
        CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
        CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
        CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
      ),
    )

    val settings = CassandraSettings.configureSink(config)

    settings.ignoredError shouldBe IgnoredErrorMode.Driver
  }

  test("throws ConfigException for invalid error mode") {
    a[ConfigException] should be thrownBy {
      val config = CassandraConfig(
        Map(
          CassandraConfigConstants.CONNECTOR_NAME_KEY       -> "test-connector",
          CassandraConfigConstants.CONTACT_POINTS           -> "localhost:9042",
          CassandraConfigConstants.PORT                     -> "9042",
          CassandraConfigConstants.MAX_CONCURRENT_REQUESTS  -> "100",
          CassandraConfigConstants.CONNECTION_POOL_SIZE     -> "2",
          CassandraConfigConstants.COMPRESSION              -> "NONE",
          CassandraConfigConstants.QUERY_TIMEOUT            -> "20000",
          CassandraConfigConstants.MAX_BATCH_SIZE           -> "64",
          CassandraConfigConstants.AUTH_PROVIDER            -> "None",
          CassandraConfigConstants.SSL_ENABLED              -> "false",
          CassandraConfigConstants.IGNORE_ERRORS_MODE       -> "invalid",
          CassandraConfigConstants.NBR_OF_RETRIES           -> "20",
          CassandraConfigConstants.ERROR_RETRY_INTERVAL     -> "60000",
          CassandraConfigConstants.KCQL                     -> "INSERT INTO testks.testtable SELECT * FROM testtopic",
          CassandraConfigConstants.PROGRESS_COUNTER_ENABLED -> "false",
        ),
      )
      CassandraSettings.configureSink(config)
    }
  }
}
