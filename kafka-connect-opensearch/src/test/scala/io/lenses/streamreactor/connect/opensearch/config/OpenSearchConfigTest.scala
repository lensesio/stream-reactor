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
package io.lenses.streamreactor.connect.opensearch.config

import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonConfigConstants
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import org.apache.kafka.common.config.ConfigException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.CollectionHasAsScala

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class OpenSearchConfigTest extends AnyFunSuite with Matchers {

  private val minProps = Map(
    HOSTS   -> "localhost",
    ES_PORT -> "9200",
    KCQL    -> "INSERT INTO idx SELECT * FROM topic",
  )

  private def settings(extra: (String, String)*): OpenSearchSettings =
    OpenSearchSettings(OpenSearchConfig(minProps ++ extra.toMap))

  // ---- Positive: default no-auth ----

  test("defaults parse without error") {
    val s = settings()
    s.hosts shouldBe Seq("localhost")
    s.port shouldBe 9200
    s.protocol shouldBe "http"
    s.awsSigningEnabled shouldBe false
    s.jwtTokenSource shouldBe None
    s.strictItemErrors shouldBe true
  }

  // ---- tablePrefix rejection on all transports ----

  test("tableprefix is rejected on http transport") {
    val ex = intercept[ConfigException](settings(ES_PREFIX -> "myprefix"))
    ex.getMessage should include("connect.opensearch.tableprefix is not supported")
  }

  test("tableprefix is rejected with SigV4") {
    val ex = intercept[ConfigException](
      settings(
        ES_PREFIX               -> "myprefix",
        AWS_SIGNING_ENABLED_KEY -> "true",
        AWS_REGION_KEY          -> "us-east-1",
        HOSTS                   -> "my-cluster.us-east-1.es.amazonaws.com",
      ),
    )
    ex.getMessage should include("connect.opensearch.tableprefix is not supported")
  }

  // ---- JWT validation ----

  test("JWT static and JWT file are mutually exclusive") {
    val ex = intercept[ConfigException](settings(
      JWT_TOKEN_KEY      -> "static-token",
      JWT_TOKEN_FILE_KEY -> "/tmp/token.jwt",
    ))
    ex.getMessage should include("mutually exclusive")
  }

  test("JWT file requires refresh interval > 0") {
    val ex = intercept[ConfigException](settings(
      JWT_TOKEN_FILE_KEY       -> "/tmp/token.jwt",
      JWT_REFRESH_INTERVAL_KEY -> "0",
    ))
    ex.getMessage should include("refresh.interval.ms must be > 0")
  }

  test("JWT and basic auth are mutually exclusive") {
    val ex = intercept[ConfigException](settings(
      JWT_TOKEN_KEY                   -> "static-token",
      CLIENT_HTTP_BASIC_AUTH_USERNAME -> "user",
      CLIENT_HTTP_BASIC_AUTH_PASSWORD -> "pass",
    ))
    ex.getMessage should include("mutually exclusive")
  }

  // ---- SigV4 validation ----

  test("SigV4 and basic auth are mutually exclusive") {
    val ex = intercept[ConfigException](
      settings(
        AWS_SIGNING_ENABLED_KEY         -> "true",
        AWS_REGION_KEY                  -> "us-east-1",
        HOSTS                           -> "my-cluster.us-east-1.es.amazonaws.com",
        CLIENT_HTTP_BASIC_AUTH_USERNAME -> "user",
        CLIENT_HTTP_BASIC_AUTH_PASSWORD -> "pass",
      ),
    )
    ex.getMessage should include("mutually exclusive")
  }

  test("SigV4 and JWT are mutually exclusive") {
    val ex = intercept[ConfigException](
      settings(
        AWS_SIGNING_ENABLED_KEY -> "true",
        AWS_REGION_KEY          -> "us-east-1",
        HOSTS                   -> "my-cluster.us-east-1.es.amazonaws.com",
        JWT_TOKEN_KEY           -> "some-token",
      ),
    )
    ex.getMessage should include("mutually exclusive")
  }

  test("SigV4 requires region") {
    val ex = intercept[ConfigException](settings(
      AWS_SIGNING_ENABLED_KEY -> "true",
      HOSTS                   -> "my-cluster.us-east-1.es.amazonaws.com",
    ))
    ex.getMessage should include("aws.region is required")
  }

  test("SigV4 rejects unknown region") {
    val ex = intercept[ConfigException](
      settings(
        AWS_SIGNING_ENABLED_KEY -> "true",
        AWS_REGION_KEY          -> "xx-neverland-99",
        HOSTS                   -> "my-cluster.us-east-1.es.amazonaws.com",
      ),
    )
    ex.getMessage should include("not a known AWS region")
  }

  test("SigV4 service must be es or aoss") {
    val ex = intercept[ConfigException](
      settings(
        AWS_SIGNING_ENABLED_KEY -> "true",
        AWS_REGION_KEY          -> "us-east-1",
        AWS_SIGNING_SERVICE_KEY -> "invalid-service",
        HOSTS                   -> "my-cluster.us-east-1.es.amazonaws.com",
      ),
    )
    ex.getMessage should include("{es, aoss}")
  }

  test("SigV4 STATIC credentials require access key id") {
    val ex = intercept[ConfigException](
      settings(
        AWS_SIGNING_ENABLED_KEY      -> "true",
        AWS_REGION_KEY               -> "us-east-1",
        AWS_CREDENTIALS_PROVIDER_KEY -> "STATIC",
        HOSTS                        -> "my-cluster.us-east-1.es.amazonaws.com",
      ),
    )
    ex.getMessage should include("aws.access.key.id is required")
  }

  test("SigV4 STATIC credentials require secret key when access key id provided") {
    val ex = intercept[ConfigException](
      settings(
        AWS_SIGNING_ENABLED_KEY      -> "true",
        AWS_REGION_KEY               -> "us-east-1",
        AWS_CREDENTIALS_PROVIDER_KEY -> "STATIC",
        AWS_ACCESS_KEY_ID_KEY        -> "AKIAIOSFODNN7EXAMPLE",
        HOSTS                        -> "my-cluster.us-east-1.es.amazonaws.com",
      ),
    )
    ex.getMessage should include("aws.secret.access.key is required")
  }

  test("SigV4 requires exactly one host") {
    val ex = intercept[ConfigException](
      settings(
        AWS_SIGNING_ENABLED_KEY -> "true",
        AWS_REGION_KEY          -> "us-east-1",
        HOSTS                   -> "host1.es.amazonaws.com,host2.es.amazonaws.com",
      ),
    )
    ex.getMessage should include("exactly one entry")
  }

  test("SigV4 rejects host with scheme prefix") {
    val ex = intercept[ConfigException](
      settings(
        AWS_SIGNING_ENABLED_KEY -> "true",
        AWS_REGION_KEY          -> "us-east-1",
        HOSTS                   -> "https://my-cluster.us-east-1.es.amazonaws.com",
      ),
    )
    ex.getMessage should include("strip the scheme")
  }

  // ---- Positive: mTLS combinations ----

  test("mTLS + basic auth parse cleanly over HTTPS") {
    // No keystore/truststore files exist in test environment, so just verify config parses without auth errors.
    // protocol=https is required when credentials are configured.
    val s = settings(
      PROTOCOL                        -> "https",
      CLIENT_HTTP_BASIC_AUTH_USERNAME -> "user",
      CLIENT_HTTP_BASIC_AUTH_PASSWORD -> "pass",
    )
    s.common.httpBasicAuthUsername shouldBe "user"
    s.common.httpBasicAuthPassword shouldBe "pass"
    s.jwtTokenSource shouldBe None
    s.awsSigningEnabled shouldBe false
  }

  test("JWT static token source created over HTTPS") {
    val s = settings(PROTOCOL -> "https", JWT_TOKEN_KEY -> "my-jwt-token")
    s.jwtTokenSource should be(defined)
    s.common.httpBasicAuthUsername shouldBe ""
  }

  test("JWT file token source created with valid refresh interval over HTTPS") {
    val tmpJwt = Files.createTempFile("test-jwt", ".jwt")
    Files.write(tmpJwt, "my-test-jwt-token".getBytes(StandardCharsets.UTF_8))
    val s = settings(
      PROTOCOL                 -> "https",
      JWT_TOKEN_FILE_KEY       -> tmpJwt.toString,
      JWT_REFRESH_INTERVAL_KEY -> "30000",
    )
    s.jwtTokenSource should be(defined)
  }

  // ---- SigV4 positive ----

  test("SigV4 DEFAULT credentials parse cleanly") {
    val s = settings(
      AWS_SIGNING_ENABLED_KEY       -> "true",
      AWS_REGION_KEY                -> "us-east-1",
      HOSTS                         -> "my-cluster.us-east-1.es.amazonaws.com",
      "connect.opensearch.protocol" -> "https",
    )
    s.awsSigningEnabled shouldBe true
    s.awsRegion shouldBe "us-east-1"
    s.awsSigningService shouldBe "es"
    s.sigV4Host shouldBe "my-cluster.us-east-1.es.amazonaws.com"
  }

  test("SigV4 aoss service accepted") {
    val s = settings(
      AWS_SIGNING_ENABLED_KEY       -> "true",
      AWS_REGION_KEY                -> "us-east-1",
      AWS_SIGNING_SERVICE_KEY       -> "aoss",
      HOSTS                         -> "my-cluster.us-east-1.aoss.amazonaws.com",
      "connect.opensearch.protocol" -> "https",
    )
    s.awsSigningService shouldBe "aoss"
  }

  test("SigV4 rejects protocol=http (signing over plain HTTP exposes credentials)") {
    val ex = intercept[ConfigException](
      settings(
        AWS_SIGNING_ENABLED_KEY       -> "true",
        AWS_REGION_KEY                -> "us-east-1",
        HOSTS                         -> "my-cluster.us-east-1.es.amazonaws.com",
        "connect.opensearch.protocol" -> "http",
      ),
    )
    ex.getMessage should include("must be 'https'")
  }

  test("strictItemErrors defaults to true") {
    settings().strictItemErrors shouldBe true
  }

  test("strictItemErrors can be set to false") {
    settings(BULK_STRICT_ITEM_ERRORS_KEY -> "false").strictItemErrors shouldBe false
  }

  test("write.timeout of 60 milliseconds is accepted (OpenSearch has always used milliseconds)") {
    settings(WRITE_TIMEOUT -> "60").common.writeTimeout shouldBe 60
  }

  test("write.timeout of 750 milliseconds is accepted") {
    settings(WRITE_TIMEOUT -> "750").common.writeTimeout shouldBe 750
  }

  // ---- KCQL splitter ----

  test("single KCQL statement parses correctly") {
    val s = settings()
    s.common.kcqls should have size 1
    s.common.kcqls.head.getSource shouldBe "topic"
    s.common.kcqls.head.getTarget shouldBe "idx"
  }

  test("two KCQL statements separated by semicolon both parse") {
    val s = OpenSearchSettings(
      OpenSearchConfig(
        Map(
          HOSTS   -> "localhost",
          ES_PORT -> "9200",
          KCQL    -> "INSERT INTO idx1 SELECT * FROM topic1;INSERT INTO idx2 SELECT * FROM topic2",
        ),
      ),
    )
    s.common.kcqls should have size 2
    s.common.kcqls.map(_.getSource) should contain allOf ("topic1", "topic2")
    s.common.kcqls.map(_.getTarget) should contain allOf ("idx1", "idx2")
  }

  test("field list with commas inside a KCQL clause is not mis-split") {
    val s = OpenSearchSettings(
      OpenSearchConfig(Map(
        HOSTS   -> "localhost",
        ES_PORT -> "9200",
        KCQL    -> "INSERT INTO idx SELECT field1, field2, field3 FROM topic",
      )),
    )
    s.common.kcqls should have size 1
    s.common.kcqls.head.getSource shouldBe "topic"
  }

  test("trailing semicolon does not produce an empty KCQL entry") {
    val s = OpenSearchSettings(OpenSearchConfig(Map(
      HOSTS   -> "localhost",
      ES_PORT -> "9200",
      KCQL    -> "INSERT INTO idx SELECT * FROM topic;",
    )))
    s.common.kcqls should have size 1
  }

  // ---- Cleartext-auth guard (Bug 3) ----

  test("protocol=http + basic auth is rejected by default") {
    val ex = intercept[ConfigException](
      settings(
        CLIENT_HTTP_BASIC_AUTH_USERNAME -> "user",
        CLIENT_HTTP_BASIC_AUTH_PASSWORD -> "pass",
      ),
    )
    ex.getMessage should include("cleartext")
    ex.getMessage should include(ALLOW_INSECURE_AUTH_KEY)
  }

  test("protocol=http + JWT static token is rejected by default") {
    val ex = intercept[ConfigException](
      settings(JWT_TOKEN_KEY -> "some-token"),
    )
    ex.getMessage should include("cleartext")
    ex.getMessage should include(ALLOW_INSECURE_AUTH_KEY)
  }

  test("protocol=http + JWT token file is rejected by default") {
    val ex = intercept[ConfigException](
      settings(
        JWT_TOKEN_FILE_KEY       -> "/tmp/token.jwt",
        JWT_REFRESH_INTERVAL_KEY -> "60000",
      ),
    )
    ex.getMessage should include("cleartext")
    ex.getMessage should include(ALLOW_INSECURE_AUTH_KEY)
  }

  test("protocol=http + basic auth + allow.insecure.auth=true parses successfully") {
    val s = settings(
      CLIENT_HTTP_BASIC_AUTH_USERNAME -> "user",
      CLIENT_HTTP_BASIC_AUTH_PASSWORD -> "pass",
      ALLOW_INSECURE_AUTH_KEY         -> "true",
    )
    s.common.httpBasicAuthUsername shouldBe "user"
    s.protocol shouldBe "http"
  }

  test("protocol=https + basic auth parses without needing opt-out") {
    val s = settings(
      PROTOCOL                        -> "https",
      CLIENT_HTTP_BASIC_AUTH_USERNAME -> "user",
      CLIENT_HTTP_BASIC_AUTH_PASSWORD -> "pass",
    )
    s.common.httpBasicAuthUsername shouldBe "user"
    s.protocol shouldBe "https"
  }

  test("protocol=http with no auth configured is accepted") {
    val s = settings()
    s.protocol shouldBe "http"
    s.common.httpBasicAuthUsername shouldBe ""
    s.jwtTokenSource shouldBe None
  }

  // ---- Progress-counter key ----

  test("connect.progress.enabled key is registered in the OpenSearch ConfigDef") {
    val configKeys = OpenSearchConfig.config.configKeys().keySet().asScala
    configKeys should contain(ElasticCommonConfigConstants.PROGRESS_COUNTER_ENABLED)
  }

  test("connect.progress.enabled key value is the bare unprefixed string") {
    ElasticCommonConfigConstants.PROGRESS_COUNTER_ENABLED shouldBe "connect.progress.enabled"
  }
}
