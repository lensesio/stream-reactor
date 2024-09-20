/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.http.sink
import cyclops.control.Option.{ none => cynone }
import cyclops.control.Option.{ some => cysome }
import io.lenses.streamreactor.common.security.StoresInfo
import io.lenses.streamreactor.connect.http.sink.client.HttpMethod.Put
import io.lenses.streamreactor.connect.http.sink.client.BasicAuthentication
import io.lenses.streamreactor.connect.http.sink.client.NoAuthentication
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef.ErrorThresholdDefault
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef.UploadSyncPeriodDefault
import io.lenses.streamreactor.connect.http.sink.config._
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
class HttpSinkConfigTest extends AnyFunSuiteLike with Matchers with EitherValues {

  val ERROR_REPORTING_ENABLED_PROP   = "connect.reporting.error.config.enabled";
  val SUCCESS_REPORTING_ENABLED_PROP = "connect.reporting.success.config.enabled";

  val DEFAULT_SSL_PROTOCOL_TLS = "TLSv1.3"
  test("should read minimal config") {

    val httpSinkConfig = HttpSinkConfig.from(
      Map(
        HttpSinkConfigDef.HttpMethodProp         -> "put",
        HttpSinkConfigDef.HttpEndpointProp       -> "http://myaddress.example.com",
        HttpSinkConfigDef.HttpRequestContentProp -> "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
        ERROR_REPORTING_ENABLED_PROP             -> "false",
        SUCCESS_REPORTING_ENABLED_PROP           -> "false",
      ),
    ).value

    httpSinkConfig.method should be(Put)
    httpSinkConfig.endpoint should be("http://myaddress.example.com")
    httpSinkConfig.content should be(
      "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
    )
    httpSinkConfig.authentication should be(NoAuthentication)
    httpSinkConfig.headers should be(List(
      ("Content-Type", "application/json"),
    ))
    httpSinkConfig.ssl should be(new StoresInfo(cysome(DEFAULT_SSL_PROTOCOL_TLS), cynone(), cynone()))
    httpSinkConfig.batch should be(BatchConfig(Some(1), None, None))
    httpSinkConfig.errorThreshold should be(ErrorThresholdDefault)
    httpSinkConfig.uploadSyncPeriod should be(UploadSyncPeriodDefault)
    httpSinkConfig.retries should be(
      RetriesConfig(
        HttpSinkConfigDef.RetriesMaxRetriesDefault,
        HttpSinkConfigDef.RetriesMaxTimeoutMsDefault,
        HttpSinkConfigDef.RetriesOnStatusCodesDefault,
      ),
    )
    httpSinkConfig.timeout should be(TimeoutConfig(HttpSinkConfigDef.ConnectionTimeoutMsDefault))
    httpSinkConfig.errorReportingController == null shouldBe false
    httpSinkConfig.successReportingController == null shouldBe false
  }

  test("fails if the method is not supported") {
    HttpSinkConfig.from(
      Map(
        HttpSinkConfigDef.HttpMethodProp         -> "NotSupported",
        HttpSinkConfigDef.HttpEndpointProp       -> "http://myaddress.example.com",
        HttpSinkConfigDef.HttpRequestContentProp -> "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
      ),
    ).left.value.getMessage should include("Invalid HTTP method. Supported methods are: Put, Post, Patch")
  }

  test("authentication set to Basic") {
    val httpSinkConfig = HttpSinkConfig.from(
      Map(
        HttpSinkConfigDef.HttpMethodProp                  -> "put",
        HttpSinkConfigDef.HttpEndpointProp                -> "http://myaddress.example.com",
        HttpSinkConfigDef.HttpRequestContentProp          -> "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
        HttpSinkConfigDef.AuthenticationTypeProp          -> "basic",
        HttpSinkConfigDef.BasicAuthenticationUsernameProp -> "user",
        HttpSinkConfigDef.BasicAuthenticationPasswordProp -> "pass",
        ERROR_REPORTING_ENABLED_PROP                      -> "false",
        SUCCESS_REPORTING_ENABLED_PROP                    -> "false",
      ),
    ).value

    httpSinkConfig.method should be(Put)
    httpSinkConfig.endpoint should be("http://myaddress.example.com")
    httpSinkConfig.content should be(
      "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
    )
    httpSinkConfig.authentication should be(BasicAuthentication("user", "pass"))
    httpSinkConfig.headers should be(List(
      ("Content-Type", "application/json"),
    ))
    httpSinkConfig.ssl should be(new StoresInfo(cysome(DEFAULT_SSL_PROTOCOL_TLS), cynone(), cynone()))
    httpSinkConfig.batch should be(BatchConfig(Some(1), None, None))
    httpSinkConfig.errorThreshold should be(ErrorThresholdDefault)
    httpSinkConfig.uploadSyncPeriod should be(UploadSyncPeriodDefault)
    httpSinkConfig.retries should be(
      RetriesConfig(
        HttpSinkConfigDef.RetriesMaxRetriesDefault,
        HttpSinkConfigDef.RetriesMaxTimeoutMsDefault,
        HttpSinkConfigDef.RetriesOnStatusCodesDefault,
      ),
    )
    httpSinkConfig.timeout should be(TimeoutConfig(HttpSinkConfigDef.ConnectionTimeoutMsDefault))
    httpSinkConfig.errorReportingController == null shouldBe false
    httpSinkConfig.successReportingController == null shouldBe false

  }
}
