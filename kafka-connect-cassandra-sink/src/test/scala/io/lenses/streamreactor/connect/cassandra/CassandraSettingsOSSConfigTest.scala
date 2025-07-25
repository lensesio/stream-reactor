/*
 * Copyright 2017-2025 Lenses.io Ltd
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

import io.lenses.streamreactor.common.errors.NoopErrorPolicy
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CassandraSettingsOSSConfigTest extends AnyFunSuite with Matchers {

  test("getOSSCassandraSinkConfig creates base configuration correctly") {
    val settings = CassandraSettings(
      connectorName         = "test-connector",
      tableSettings         = Seq.empty,
      errorPolicy           = NoopErrorPolicy(),
      taskRetries           = 20,
      enableProgress        = false,
      maxConcurrentRequests = 100,
      connectionPoolSize    = 2,
      compression           = "NONE",
      queryTimeout          = 20000,
      maxBatchSize          = 64,
      localDc               = None,
      contactPoints         = Seq("host1:9042", "host2:9043"),
      port                  = 9042,
      auth                  = None,
      ssl                   = None,
      ignoredError          = IgnoredErrorMode.None,
      driverSettings        = Map.empty,
    )

    val ossConfig = CassandraSettings.getOSSCassandraSinkConfig(settings)

    ossConfig should contain("name" -> "test-connector")
    ossConfig should contain("contactPoints" -> "host1:9042,host2:9043")
    ossConfig should contain("port" -> "9042")
    ossConfig should contain("maxConcurrentRequests" -> "100")
    ossConfig should contain("connectionPoolLocalSize" -> "2")
    ossConfig should contain("compression" -> "NONE")
    ossConfig should contain("queryExecutionTimeout" -> "20000")
    ossConfig should contain("maxNumberOfRecordsInBatch" -> "64")
    ossConfig should contain("ignoreErrors" -> "none")
    ossConfig should contain("datastax-java-driver.basic.request.timeout" -> "20000 seconds")
    ossConfig should contain("datastax-java-driver.basic.connection.pool.local.size" -> "2")
    ossConfig should not contain key("loadBalancing.localDc")
    ossConfig should not contain key("ssl.provider")
    ossConfig should not contain key("auth.provider")
  }

  test("getOSSCassandraSinkConfig includes local DC when set") {
    val settings = CassandraSettings(
      connectorName         = "test-connector",
      tableSettings         = Seq.empty,
      errorPolicy           = NoopErrorPolicy(),
      taskRetries           = 20,
      enableProgress        = false,
      maxConcurrentRequests = 100,
      connectionPoolSize    = 2,
      compression           = "NONE",
      queryTimeout          = 20000,
      maxBatchSize          = 64,
      localDc               = Some("datacenter1"),
      contactPoints         = Seq("host1:9042"),
      port                  = 9042,
      auth                  = None,
      ssl                   = None,
      ignoredError          = IgnoredErrorMode.None,
      driverSettings        = Map.empty,
    )

    val ossConfig = CassandraSettings.getOSSCassandraSinkConfig(settings)

    ossConfig should contain("loadBalancing.localDc" -> "datacenter1")
  }

  test("getOSSCassandraSinkConfig includes SSL configuration when enabled") {
    val sslSettings = CassandraSSLSettings(
      provider             = Some("OpenSSL"),
      trustStorePath       = Some("/path/to/truststore.jks"),
      trustStorePass       = Some("trustpass123"),
      keyStorePath         = Some("/path/to/keystore.jks"),
      keyStorePass         = Some("keypass123"),
      cipherSuites         = Some("TLS_RSA_WITH_AES_128_CBC_SHA"),
      hostnameVerification = false,
      openSslKeyCertChain  = Some("/path/to/cert.pem"),
      openSslPrivateKey    = Some("/path/to/key.pem"),
    )

    val settings = CassandraSettings(
      connectorName         = "test-connector",
      tableSettings         = Seq.empty,
      errorPolicy           = NoopErrorPolicy(),
      taskRetries           = 20,
      enableProgress        = false,
      maxConcurrentRequests = 100,
      connectionPoolSize    = 2,
      compression           = "NONE",
      queryTimeout          = 20000,
      maxBatchSize          = 64,
      localDc               = None,
      contactPoints         = Seq("host1:9042"),
      port                  = 9042,
      auth                  = None,
      ssl                   = Some(sslSettings),
      ignoredError          = IgnoredErrorMode.None,
      driverSettings        = Map.empty,
    )

    val ossConfig = CassandraSettings.getOSSCassandraSinkConfig(settings)

    ossConfig should contain("ssl.provider" -> "OpenSSL")
    ossConfig should contain("ssl.truststore.path" -> "/path/to/truststore.jks")
    ossConfig should contain("ssl.truststore.password" -> "trustpass123")
    ossConfig should contain("ssl.keystore.path" -> "/path/to/keystore.jks")
    ossConfig should contain("ssl.keystore.password" -> "keypass123")
    ossConfig should contain("ssl.cipherSuites" -> "TLS_RSA_WITH_AES_128_CBC_SHA")
    ossConfig should contain("ssl.hostnameValidation" -> "false")
    ossConfig should contain("ssl.openssl.keyCertChain" -> "/path/to/cert.pem")
    ossConfig should contain("ssl.openssl.privateKey" -> "/path/to/key.pem")
  }

  test("getOSSCassandraSinkConfig includes PLAIN authentication configuration") {
    val authSettings = CassandraAuthSettings(
      provider        = "PLAIN",
      username        = Some("cassandra_user"),
      password        = Some("secure_password_123"),
      gssapiKeyTab    = None,
      gssapiPrincipal = None,
      gssapiService   = "dse",
    )

    val settings = CassandraSettings(
      connectorName         = "test-connector",
      tableSettings         = Seq.empty,
      errorPolicy           = NoopErrorPolicy(),
      taskRetries           = 20,
      enableProgress        = false,
      maxConcurrentRequests = 100,
      connectionPoolSize    = 2,
      compression           = "NONE",
      queryTimeout          = 20000,
      maxBatchSize          = 64,
      localDc               = None,
      contactPoints         = Seq("host1:9042"),
      port                  = 9042,
      auth                  = Some(authSettings),
      ssl                   = None,
      ignoredError          = IgnoredErrorMode.None,
      driverSettings        = Map.empty,
    )

    val ossConfig = CassandraSettings.getOSSCassandraSinkConfig(settings)

    ossConfig should contain("auth.provider" -> "PLAIN")
    ossConfig should contain("auth.username" -> "cassandra_user")
    ossConfig should contain("auth.password" -> "secure_password_123")
    ossConfig should contain("auth.gssapi.service" -> "dse")
  }

  test("getOSSCassandraSinkConfig includes topic mappings when table settings have field mappings") {
    val tableSetting = CassandraTableSetting(
      namespace        = "testks",
      table            = "testtable",
      sourceKafkaTopic = "testtopic",
      fieldMappings = Seq(
        CassandraTableSettingFieldMapping("id", "key_id", "key"),
        CassandraTableSettingFieldMapping("trace_id", "trace_id", "header"),
        CassandraTableSettingFieldMapping("name", "user_name", "value"),
        CassandraTableSettingFieldMapping("age", "user_age", "value"),
      ),
      others = Map(
        "behavior.on.null.values" -> "IGNORE",
        "consistency.level"       -> "ONE",
      ),
    )

    val settings = CassandraSettings(
      connectorName         = "test-connector",
      tableSettings         = Seq(tableSetting),
      errorPolicy           = NoopErrorPolicy(),
      taskRetries           = 20,
      enableProgress        = false,
      maxConcurrentRequests = 100,
      connectionPoolSize    = 2,
      compression           = "NONE",
      queryTimeout          = 20000,
      maxBatchSize          = 64,
      localDc               = None,
      contactPoints         = Seq("host1:9042"),
      port                  = 9042,
      auth                  = None,
      ssl                   = None,
      ignoredError          = IgnoredErrorMode.None,
      driverSettings        = Map.empty,
    )

    val ossConfig = CassandraSettings.getOSSCassandraSinkConfig(settings)

    // Verify base config
    ossConfig should contain("name" -> "test-connector")
    ossConfig should contain("contactPoints" -> "host1:9042")

    // Verify topic mapping
    ossConfig should contain(
      "topic.testtopic.testks.testtable.mapping" -> "\"key_id\"=\"key.id\", \"trace_id\"=\"header.trace_id\", \"user_name\"=\"value.name\", \"user_age\"=\"value.age\"",
    )

    // Verify extra settings
    ossConfig should contain("topic.testtopic.testks.testtable.behavior.on.null.values" -> "IGNORE")
    ossConfig should contain("topic.testtopic.testks.testtable.consistency.level" -> "ONE")
  }

  test("getOSSCassandraSinkConfig handles table settings without field mappings") {
    val tableSetting = CassandraTableSetting(
      namespace        = "testks",
      table            = "testtable",
      sourceKafkaTopic = "testtopic",
      fieldMappings    = Seq.empty,
      others           = Map("consistency.level" -> "QUORUM"),
    )

    val settings = CassandraSettings(
      connectorName         = "test-connector",
      tableSettings         = Seq(tableSetting),
      errorPolicy           = NoopErrorPolicy(),
      taskRetries           = 20,
      enableProgress        = false,
      maxConcurrentRequests = 100,
      connectionPoolSize    = 2,
      compression           = "NONE",
      queryTimeout          = 20000,
      maxBatchSize          = 64,
      localDc               = None,
      contactPoints         = Seq("host1:9042"),
      port                  = 9042,
      auth                  = None,
      ssl                   = None,
      ignoredError          = IgnoredErrorMode.None,
      driverSettings        = Map.empty,
    )

    val ossConfig = CassandraSettings.getOSSCassandraSinkConfig(settings)

    // Verify base config
    ossConfig should contain("name" -> "test-connector")

    // Verify no topic mapping (since no field mappings)
    ossConfig should not contain key("topic.testtopic.testks.testtable.mapping")

    // Verify extra settings
    ossConfig should contain("topic.testtopic.testks.testtable.consistency.level" -> "QUORUM")
  }

  test("getOSSCassandraSinkConfig handles multiple table settings") {
    val tableSetting1 = CassandraTableSetting(
      namespace        = "testks1",
      table            = "table1",
      sourceKafkaTopic = "topic1",
      fieldMappings = Seq(
        CassandraTableSettingFieldMapping("id", "record_id", "value"),
        CassandraTableSettingFieldMapping("name", "record_name", "value"),
      ),
      others = Map("consistency.level" -> "QUORUM"),
    )

    val tableSetting2 = CassandraTableSetting(
      namespace        = "testks2",
      table            = "table2",
      sourceKafkaTopic = "topic2",
      fieldMappings = Seq(
        CassandraTableSettingFieldMapping("id", "key_id", "key"),
        CassandraTableSettingFieldMapping("value", "record_value", "value"),
      ),
      others = Map(
        "behavior.on.null.values" -> "IGNORE",
        "ttl"                     -> "86400",
      ),
    )

    val settings = CassandraSettings(
      connectorName         = "test-connector",
      tableSettings         = Seq(tableSetting1, tableSetting2),
      errorPolicy           = NoopErrorPolicy(),
      taskRetries           = 20,
      enableProgress        = false,
      maxConcurrentRequests = 100,
      connectionPoolSize    = 2,
      compression           = "NONE",
      queryTimeout          = 20000,
      maxBatchSize          = 64,
      localDc               = None,
      contactPoints         = Seq("host1:9042"),
      port                  = 9042,
      auth                  = None,
      ssl                   = None,
      ignoredError          = IgnoredErrorMode.None,
      driverSettings        = Map.empty,
    )

    val ossConfig = CassandraSettings.getOSSCassandraSinkConfig(settings)

    // Verify base config
    ossConfig should contain("name" -> "test-connector")

    // Verify topic mappings for both tables
    ossConfig should contain(
      "topic.topic1.testks1.table1.mapping" -> "\"record_id\"=\"value.id\", \"record_name\"=\"value.name\"",
    )
    ossConfig should contain(
      "topic.topic2.testks2.table2.mapping" -> "\"key_id\"=\"key.id\", \"record_value\"=\"value.value\"",
    )

    // Verify extra settings for both tables
    ossConfig should contain("topic.topic1.testks1.table1.consistency.level" -> "QUORUM")
    ossConfig should contain("topic.topic2.testks2.table2.behavior.on.null.values" -> "IGNORE")
    ossConfig should contain("topic.topic2.testks2.table2.ttl" -> "86400")
  }

  test("getOSSCassandraSinkConfig includes driver settings") {
    val settings = CassandraSettings(
      connectorName         = "test-connector",
      tableSettings         = Seq.empty,
      errorPolicy           = NoopErrorPolicy(),
      taskRetries           = 20,
      enableProgress        = false,
      maxConcurrentRequests = 100,
      connectionPoolSize    = 2,
      compression           = "NONE",
      queryTimeout          = 20000,
      maxBatchSize          = 64,
      localDc               = None,
      contactPoints         = Seq("host1:9042"),
      port                  = 9042,
      auth                  = None,
      ssl                   = None,
      ignoredError          = IgnoredErrorMode.None,
      driverSettings = Map(
        "driver.custom.setting"  -> "custom_value",
        "driver.another.setting" -> "another_value",
      ),
    )

    val ossConfig = CassandraSettings.getOSSCassandraSinkConfig(settings)

    // Verify base config
    ossConfig should contain("name" -> "test-connector")

    // Verify driver settings are included
    ossConfig should contain("driver.custom.setting" -> "custom_value")
    ossConfig should contain("driver.another.setting" -> "another_value")
  }

  test("getOSSCassandraSinkConfig combines all configurations correctly") {
    val sslSettings = CassandraSSLSettings(
      provider             = Some("JDK"),
      trustStorePath       = Some("/path/to/truststore.jks"),
      trustStorePass       = Some("trustpass123"),
      keyStorePath         = None,
      keyStorePass         = None,
      cipherSuites         = None,
      hostnameVerification = true,
      openSslKeyCertChain  = None,
      openSslPrivateKey    = None,
    )

    val authSettings = CassandraAuthSettings(
      provider        = "PLAIN",
      username        = Some("cassandra_user"),
      password        = Some("secure_password_123"),
      gssapiKeyTab    = None,
      gssapiPrincipal = None,
      gssapiService   = "dse",
    )

    val tableSetting = CassandraTableSetting(
      namespace        = "testks",
      table            = "testtable",
      sourceKafkaTopic = "testtopic",
      fieldMappings = Seq(
        CassandraTableSettingFieldMapping("id", "key_id", "key"),
        CassandraTableSettingFieldMapping("name", "user_name", "value"),
      ),
      others = Map("consistency.level" -> "LOCAL_QUORUM"),
    )

    val settings = CassandraSettings(
      connectorName         = "test-connector",
      tableSettings         = Seq(tableSetting),
      errorPolicy           = NoopErrorPolicy(),
      taskRetries           = 20,
      enableProgress        = false,
      maxConcurrentRequests = 100,
      connectionPoolSize    = 2,
      compression           = "NONE",
      queryTimeout          = 20000,
      maxBatchSize          = 64,
      localDc               = Some("datacenter1"),
      contactPoints         = Seq("host1:9042", "host2:9043"),
      port                  = 9042,
      auth                  = Some(authSettings),
      ssl                   = Some(sslSettings),
      ignoredError          = IgnoredErrorMode.Driver,
      driverSettings        = Map("driver.custom.setting" -> "custom_value"),
    )

    val ossConfig = CassandraSettings.getOSSCassandraSinkConfig(settings)

    // Base config
    ossConfig should contain("name" -> "test-connector")
    ossConfig should contain("contactPoints" -> "host1:9042,host2:9043")
    ossConfig should contain("port" -> "9042")
    ossConfig should contain("maxConcurrentRequests" -> "100")
    ossConfig should contain("connectionPoolLocalSize" -> "2")
    ossConfig should contain("compression" -> "NONE")
    ossConfig should contain("queryExecutionTimeout" -> "20000")
    ossConfig should contain("maxNumberOfRecordsInBatch" -> "64")
    ossConfig should contain("ignoreErrors" -> "driver")

    // Local DC
    ossConfig should contain("loadBalancing.localDc" -> "datacenter1")

    // SSL config
    ossConfig should contain("ssl.provider" -> "JDK")
    ossConfig should contain("ssl.truststore.path" -> "/path/to/truststore.jks")
    ossConfig should contain("ssl.truststore.password" -> "trustpass123")
    ossConfig should contain("ssl.hostnameValidation" -> "true")

    // Auth config
    ossConfig should contain("auth.provider" -> "PLAIN")
    ossConfig should contain("auth.username" -> "cassandra_user")
    ossConfig should contain("auth.password" -> "secure_password_123")
    ossConfig should contain("auth.gssapi.service" -> "dse")

    // Topic mapping
    ossConfig should contain(
      "topic.testtopic.testks.testtable.mapping" -> "\"key_id\"=\"key.id\", \"user_name\"=\"value.name\"",
    )

    // Extra settings
    ossConfig should contain("topic.testtopic.testks.testtable.consistency.level" -> "LOCAL_QUORUM")

    // Driver settings
    ossConfig should contain("driver.custom.setting" -> "custom_value")
  }
}
