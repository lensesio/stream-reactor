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
package io.lenses.streamreactor.connect.http.sink
import cyclops.control.Option.{ none => cynone }
import cyclops.control.Option.{ some => cysome }
import io.lenses.streamreactor.common.batch.Count
import io.lenses.streamreactor.common.batch.HttpBatchPolicy
import io.lenses.streamreactor.common.security.StoresInfo
import io.lenses.streamreactor.connect.http.sink.client.HttpMethod.Put
import io.lenses.streamreactor.connect.http.sink.client.BasicAuthentication
import io.lenses.streamreactor.connect.http.sink.client.NoAuthentication
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef.ErrorThresholdDefault
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef.UploadSyncPeriodDefault
import io.lenses.streamreactor.connect.http.sink.config._
import org.apache.kafka.common.config.ConfigException
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import io.lenses.streamreactor.connect.http.sink.tpl.Headers
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
        HttpSinkConfigDef.RequestTimeoutMsProp   -> "123",
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
    httpSinkConfig.headers should be(Headers(List(
                                               ("Content-Type", "application/json"),
                                             ),
                                             false,
    ))
    httpSinkConfig.ssl should be(new StoresInfo(cysome(DEFAULT_SSL_PROTOCOL_TLS), cynone(), cynone()))
    httpSinkConfig.batch should be(BatchConfig(Some(1), None, None))
    httpSinkConfig.errorThreshold should be(ErrorThresholdDefault)
    httpSinkConfig.uploadSyncPeriod should be(UploadSyncPeriodDefault)
    httpSinkConfig.retries should be(
      ExponentialRetryConfig(
        HttpSinkConfigDef.RetriesMaxRetriesDefault,
        HttpSinkConfigDef.RetriesMaxTimeoutMsDefault,
        HttpSinkConfigDef.RetriesOnStatusCodesDefault,
      ),
    )
    httpSinkConfig.timeout should be(
      TimeoutConfig(HttpSinkConfigDef.ConnectionTimeoutMsDefault, 123),
    )
    httpSinkConfig.tidyJson should be(false)
    httpSinkConfig.errorReportingController == null shouldBe false
    httpSinkConfig.successReportingController == null shouldBe false
  }

  test("request timeout must be positive") {
    for (timeout <- List("0", "-1")) {
      val error = intercept[ConfigException] {
        HttpSinkConfig.from(
          Map(
            HttpSinkConfigDef.HttpMethodProp         -> "post",
            HttpSinkConfigDef.HttpEndpointProp       -> "http://example.test",
            HttpSinkConfigDef.HttpRequestContentProp -> "content",
            HttpSinkConfigDef.RequestTimeoutMsProp   -> timeout,
            ERROR_REPORTING_ENABLED_PROP             -> "false",
            SUCCESS_REPORTING_ENABLED_PROP           -> "false",
          ),
        )
      }
      error.getMessage should include(HttpSinkConfigDef.RequestTimeoutMsProp)
    }
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

  test("fails when retries is invalid") {
    HttpSinkConfig.from(
      Map(
        HttpSinkConfigDef.HttpMethodProp           -> "put",
        HttpSinkConfigDef.HttpEndpointProp         -> "http://myaddress.example.com",
        HttpSinkConfigDef.HttpRequestContentProp   -> "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
        HttpSinkConfigDef.RetryModeProp            -> "invalid",
        HttpSinkConfigDef.RetriesMaxRetriesProp    -> "10",
        HttpSinkConfigDef.RetriesOnStatusCodesProp -> "500,502",
        ERROR_REPORTING_ENABLED_PROP               -> "false",
        SUCCESS_REPORTING_ENABLED_PROP             -> "false",
      ),
    ).left.value.getMessage should include("Invalid retry mode: invalid. Expected one of: Exponential, Fixed")
  }
  test("retries is set to fixed") {
    val httpSinkConfig = HttpSinkConfig.from(
      Map(
        HttpSinkConfigDef.HttpMethodProp           -> "put",
        HttpSinkConfigDef.HttpEndpointProp         -> "http://myaddress.example.com",
        HttpSinkConfigDef.HttpRequestContentProp   -> "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
        HttpSinkConfigDef.RetryModeProp            -> "fixed",
        HttpSinkConfigDef.RetriesMaxRetriesProp    -> "10",
        HttpSinkConfigDef.FixedRetryIntervalProp   -> "111",
        HttpSinkConfigDef.RetriesOnStatusCodesProp -> "500,502",
        ERROR_REPORTING_ENABLED_PROP               -> "false",
        SUCCESS_REPORTING_ENABLED_PROP             -> "false",
        HttpSinkConfigDef.JsonTidyProp             -> "true",
      ),
    ).value

    httpSinkConfig.method should be(Put)
    httpSinkConfig.endpoint should be("http://myaddress.example.com")
    httpSinkConfig.content should be(
      "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
    )
    httpSinkConfig.authentication should be(NoAuthentication)
    httpSinkConfig.headers should be(Headers(List(
                                               ("Content-Type", "application/json"),
                                             ),
                                             false,
    ))
    httpSinkConfig.ssl should be(new StoresInfo(cysome(DEFAULT_SSL_PROTOCOL_TLS), cynone(), cynone()))
    httpSinkConfig.batch should be(BatchConfig(Some(1), None, None))
    httpSinkConfig.errorThreshold should be(ErrorThresholdDefault)
    httpSinkConfig.uploadSyncPeriod should be(UploadSyncPeriodDefault)
    httpSinkConfig.retries should be(
      FixedRetryConfig(
        10,
        111,
        List(500, 502),
      ),
    )
    httpSinkConfig.timeout should be(
      TimeoutConfig(HttpSinkConfigDef.ConnectionTimeoutMsDefault, HttpSinkConfigDef.RequestTimeoutMsDefault),
    )
    httpSinkConfig.tidyJson should be(true)
    httpSinkConfig.errorReportingController == null shouldBe false
    httpSinkConfig.successReportingController == null shouldBe false
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
        HttpSinkConfigDef.JsonTidyProp                    -> "true",
      ),
    ).value

    httpSinkConfig.method should be(Put)
    httpSinkConfig.endpoint should be("http://myaddress.example.com")
    httpSinkConfig.content should be(
      "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
    )
    httpSinkConfig.authentication should be(BasicAuthentication("user", "pass"))
    httpSinkConfig.headers should be(Headers(List(
                                               ("Content-Type", "application/json"),
                                             ),
                                             false,
    ))
    httpSinkConfig.ssl should be(new StoresInfo(cysome(DEFAULT_SSL_PROTOCOL_TLS), cynone(), cynone()))
    httpSinkConfig.batch should be(BatchConfig(Some(1), None, None))
    httpSinkConfig.errorThreshold should be(ErrorThresholdDefault)
    httpSinkConfig.uploadSyncPeriod should be(UploadSyncPeriodDefault)
    httpSinkConfig.retries should be(
      ExponentialRetryConfig(
        HttpSinkConfigDef.RetriesMaxRetriesDefault,
        HttpSinkConfigDef.RetriesMaxTimeoutMsDefault,
        HttpSinkConfigDef.RetriesOnStatusCodesDefault,
      ),
    )
    httpSinkConfig.timeout should be(
      TimeoutConfig(HttpSinkConfigDef.ConnectionTimeoutMsDefault, HttpSinkConfigDef.RequestTimeoutMsDefault),
    )
    httpSinkConfig.tidyJson should be(true)
    httpSinkConfig.errorReportingController == null shouldBe false
    httpSinkConfig.successReportingController == null shouldBe false

  }

  test("NullPayloadHandler should be NullLiteralNullPayloadHandler when configured as 'null'") {
    val httpSinkConfig = HttpSinkConfig.from(
      Map(
        HttpSinkConfigDef.HttpMethodProp         -> "put",
        HttpSinkConfigDef.HttpEndpointProp       -> "http://myaddress.example.com",
        HttpSinkConfigDef.HttpRequestContentProp -> "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
        HttpSinkConfigDef.NullPayloadHandler     -> "null",
        ERROR_REPORTING_ENABLED_PROP             -> "false",
        SUCCESS_REPORTING_ENABLED_PROP           -> "false",
      ),
    ).value

    httpSinkConfig.nullPayloadHandler should be(NullLiteralNullPayloadHandler)
  }

  test("NullPayloadHandler should be ErrorNullPayloadHandler when configured as 'error'") {
    val httpSinkConfig = HttpSinkConfig.from(
      Map(
        HttpSinkConfigDef.HttpMethodProp         -> "put",
        HttpSinkConfigDef.HttpEndpointProp       -> "http://myaddress.example.com",
        HttpSinkConfigDef.HttpRequestContentProp -> "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
        HttpSinkConfigDef.NullPayloadHandler     -> "error",
        ERROR_REPORTING_ENABLED_PROP             -> "false",
        SUCCESS_REPORTING_ENABLED_PROP           -> "false",
      ),
    ).value

    httpSinkConfig.nullPayloadHandler should be(ErrorNullPayloadHandler)
  }

  test("NullPayloadHandler should be EmptyStringNullPayloadHandler when configured as 'empty'") {
    val httpSinkConfig = HttpSinkConfig.from(
      Map(
        HttpSinkConfigDef.HttpMethodProp         -> "put",
        HttpSinkConfigDef.HttpEndpointProp       -> "http://myaddress.example.com",
        HttpSinkConfigDef.HttpRequestContentProp -> "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
        HttpSinkConfigDef.NullPayloadHandler     -> "empty",
        ERROR_REPORTING_ENABLED_PROP             -> "false",
        SUCCESS_REPORTING_ENABLED_PROP           -> "false",
      ),
    ).value

    httpSinkConfig.nullPayloadHandler should be(EmptyStringNullPayloadHandler)
  }

  test("NullPayloadHandler should be CustomNullPayloadHandler when configured as 'custom' with a custom value") {
    val customValue = "customValue"
    val httpSinkConfig = HttpSinkConfig.from(
      Map(
        HttpSinkConfigDef.HttpMethodProp           -> "put",
        HttpSinkConfigDef.HttpEndpointProp         -> "http://myaddress.example.com",
        HttpSinkConfigDef.HttpRequestContentProp   -> "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
        HttpSinkConfigDef.NullPayloadHandler       -> "custom",
        HttpSinkConfigDef.CustomNullPayloadHandler -> customValue,
        ERROR_REPORTING_ENABLED_PROP               -> "false",
        SUCCESS_REPORTING_ENABLED_PROP             -> "false",
      ),
    ).value

    httpSinkConfig.nullPayloadHandler should be(CustomNullPayloadHandler(customValue))
  }

  test("fails when max queue size is not positive") {
    // The Range validator runs while the underlying Kafka AbstractConfig is parsed, so it surfaces
    // as a thrown ConfigException rather than a Left.
    val thrown = the[ConfigException] thrownBy HttpSinkConfig.from(
      Map(
        HttpSinkConfigDef.HttpMethodProp         -> "put",
        HttpSinkConfigDef.HttpEndpointProp       -> "http://myaddress.example.com",
        HttpSinkConfigDef.HttpRequestContentProp -> "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
        HttpSinkConfigDef.MaxQueueSizeProp       -> "0",
        ERROR_REPORTING_ENABLED_PROP             -> "false",
        SUCCESS_REPORTING_ENABLED_PROP           -> "false",
      ),
    )

    thrown.getMessage should include(HttpSinkConfigDef.MaxQueueSizeProp)
  }

  test("fails when batch count exceeds max queue size") {
    val result = HttpSinkConfig.from(
      Map(
        HttpSinkConfigDef.HttpMethodProp         -> "put",
        HttpSinkConfigDef.HttpEndpointProp       -> "http://myaddress.example.com",
        HttpSinkConfigDef.HttpRequestContentProp -> "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
        HttpSinkConfigDef.BatchCountProp         -> "10",
        HttpSinkConfigDef.MaxQueueSizeProp       -> "5",
        ERROR_REPORTING_ENABLED_PROP             -> "false",
        SUCCESS_REPORTING_ENABLED_PROP           -> "false",
      ),
    )

    result.left.value shouldBe a[IllegalArgumentException]
    result.left.value.getMessage should include(HttpSinkConfigDef.BatchCountProp)
    result.left.value.getMessage should include(HttpSinkConfigDef.MaxQueueSizeProp)
  }

  test("fails when the default batch count exceeds max queue size") {
    val result = HttpSinkConfig.from(
      Map(
        HttpSinkConfigDef.HttpMethodProp         -> "put",
        HttpSinkConfigDef.HttpEndpointProp       -> "http://myaddress.example.com",
        HttpSinkConfigDef.HttpRequestContentProp -> "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
        HttpSinkConfigDef.BatchCountProp         -> "0",
        HttpSinkConfigDef.MaxQueueSizeProp       -> "5",
        ERROR_REPORTING_ENABLED_PROP             -> "false",
        SUCCESS_REPORTING_ENABLED_PROP           -> "false",
      ),
    )

    result.left.value shouldBe a[IllegalArgumentException]
    result.left.value.getMessage should include("default batch record count")
    result.left.value.getMessage should include(HttpSinkConfigDef.MaxQueueSizeProp)
  }

  test("BatchConfig.toBatchPolicy falls back to the default policy when nothing is configured") {
    BatchConfig(None, None, None).toBatchPolicy should be theSameInstanceAs HttpBatchPolicy.Default
  }

  test("BatchConfig.toBatchPolicy uses the configured conditions when present") {
    val policy = BatchConfig(Some(10L), None, None).toBatchPolicy
    policy should not be theSameInstanceAs(HttpBatchPolicy.Default)
    policy.conditions.collectFirst { case Count(c) => c } should be(Some(10L))
  }

  test("NullPayloadHandler should throw ConfigException for an invalid handler name") {
    val result = HttpSinkConfig.from(
      Map(
        HttpSinkConfigDef.HttpMethodProp         -> "put",
        HttpSinkConfigDef.HttpEndpointProp       -> "http://myaddress.example.com",
        HttpSinkConfigDef.HttpRequestContentProp -> "<note>\n<to>Dave</to>\n<from>Jason</from>\n<body>Hooray for Kafka Connect!</body>\n</note>",
        HttpSinkConfigDef.NullPayloadHandler     -> "invalidHandler",
        ERROR_REPORTING_ENABLED_PROP             -> "false",
        SUCCESS_REPORTING_ENABLED_PROP           -> "false",
      ),
    )

    result.left.value shouldBe a[ConfigException]
    result.left.value.getMessage shouldBe "Invalid null payload handler specified"
  }
}
