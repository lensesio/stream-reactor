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
package io.lenses.streamreactor.connect.http.sink.config

import cats.implicits.toBifunctorOps
import io.lenses.streamreactor.common.security.StoresInfo
import io.lenses.streamreactor.common.utils.CyclopsToScalaEither
import io.lenses.streamreactor.connect.http.sink.client.Authentication
import io.lenses.streamreactor.connect.http.sink.client.AuthenticationKeys
import io.lenses.streamreactor.connect.http.sink.client.HttpMethod
import io.lenses.streamreactor.connect.http.sink.commit._
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef.AuthenticationTypeProp
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef.BasicAuthenticationPasswordProp
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef.BasicAuthenticationUsernameProp
import io.lenses.streamreactor.connect.http.sink.reporter.converter.HttpFailureSpecificHeaderRecordConverter
import io.lenses.streamreactor.connect.http.sink.reporter.converter.HttpSuccessSpecificHeaderRecordConverter
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpFailureConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpSuccessConnectorSpecificRecordData
import io.lenses.streamreactor.connect.reporting.ReportingController.ErrorReportingController
import io.lenses.streamreactor.connect.reporting.ReportingController.SuccessReportingController
import io.lenses.streamreactor.connect.reporting.ReportingController
import io.lenses.streamreactor.connect.reporting.ReportingMessagesConfig
import io.lenses.streamreactor.connect.reporting.model.ConnectorSpecificRecordData
import io.lenses.streamreactor.connect.reporting.model.RecordConverter
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.connect.errors.ConnectException

import java.net.MalformedURLException
import java.net.URL
import java.time.Clock
import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.Try

case class BatchConfig(
  batchCount:   Option[Long],
  batchSize:    Option[Long],
  timeInterval: Option[Long],
) {

  def toBatchPolicy: BatchPolicy = {
    val conditions: Seq[BatchPolicyCondition] = Seq(
      batchCount.map(Count),
      batchSize.map(FileSize),
      timeInterval.map(inter => Interval(Duration.ofSeconds(inter), Clock.systemDefaultZone())),
    ).flatten

    BatchPolicy(conditions: _*)
  }
}

object BatchConfig {
  def from(config: AbstractConfig): BatchConfig = {
    val batchCount   = Option(config.getLong(HttpSinkConfigDef.BatchCountProp)).map(_.toLong).filter(_ > 0)
    val batchSize    = Option(config.getLong(HttpSinkConfigDef.BatchSizeProp)).map(_.toLong).filter(_ > 0)
    val timeInterval = Option(config.getLong(HttpSinkConfigDef.TimeIntervalProp)).map(_.toLong).filter(_ > 0)
    BatchConfig(batchCount, batchSize, timeInterval)
  }
}

case class TimeoutConfig(
  connectionTimeoutMs: Int,
)

sealed trait RetryMode
case object RetryMode {
  case object ExponentialRetryMode extends RetryMode
  case object FixedRetryMode       extends RetryMode

  def withNameInsensitiveEither(name: String): Either[Throwable, RetryMode] =
    name.toLowerCase match {
      case "exponential" => Right(ExponentialRetryMode)
      case "fixed"       => Right(FixedRetryMode)
      case _             => Left(new IllegalArgumentException(s"Invalid retry mode: $name. Expected one of: Exponential, Fixed"))
    }
}

sealed trait RetryConfig {
  def maxRetries:    Int
  def onStatusCodes: List[Int]
}
case class FixedRetryConfig(
  maxRetries:    Int,
  intervalMs:    Int,
  onStatusCodes: List[Int],
) extends RetryConfig {
  def name: String = "Fixed"
}

object FixedRetryConfig {

  /**
   * Creates a fixed interval retry policy
   *
   * @param interval The fixed interval between retries
   * @param maxRetry Maximum number of retry attempts
   * @return A function that takes the current retry attempt and returns the next delay as an Option
   */
  def fixedInterval(interval: FiniteDuration, maxRetry: Int): Int => Option[FiniteDuration] = {
    k => if (k > maxRetry) None else Some(interval)
  }
}
case class ExponentialRetryConfig(
  maxRetries:    Int,
  maxTimeoutMs:  Int,
  onStatusCodes: List[Int],
) extends RetryConfig {
  def name: String = "Exponential"
}

object RetriesConfig {
  def fromConfig(config: AbstractConfig): Either[Throwable, RetryConfig] = {
    val retryModeString = config.getString(HttpSinkConfigDef.RetryModeProp)
    val retryMode       = RetryMode.withNameInsensitiveEither(retryModeString)
    val onStatusCodes = Option(config.getList(HttpSinkConfigDef.RetriesOnStatusCodesProp))
      .map(_.asScala.toList)
      .filter(_.nonEmpty)
      .getOrElse(Nil)
      .map(_.toInt)
    val maxRetries = config.getInt(HttpSinkConfigDef.RetriesMaxRetriesProp)
    retryMode match {
      case Right(RetryMode.ExponentialRetryMode) =>
        val maxTimeoutMs = config.getInt(HttpSinkConfigDef.RetriesMaxTimeoutMsProp)
        Right(
          ExponentialRetryConfig(maxRetries, maxTimeoutMs, onStatusCodes),
        )
      case Right(RetryMode.FixedRetryMode) =>
        val intervalMs = config.getInt(HttpSinkConfigDef.FixedRetryIntervalProp)

        Right(FixedRetryConfig(maxRetries, intervalMs, onStatusCodes))
      case Left(e) => Left(new ConnectException(e))
    }
  }
}

case class HttpSinkConfig(
  method:                     HttpMethod,
  endpoint:                   String,
  content:                    String,
  authentication:             Authentication,
  headers:                    List[(String, String)],
  ssl:                        StoresInfo,
  batch:                      BatchConfig,
  nullPayloadHandler:         NullPayloadHandler,
  errorThreshold:             Int,
  uploadSyncPeriod:           Int,
  retries:                    RetryConfig,
  timeout:                    TimeoutConfig,
  tidyJson:                   Boolean,
  errorReportingController:   ReportingController[HttpFailureConnectorSpecificRecordData],
  successReportingController: ReportingController[HttpSuccessConnectorSpecificRecordData],
  maxQueueSize:               Int,
  maxQueueOfferTimeout:       FiniteDuration,
)

object HttpSinkConfig {
  def from(configs: Map[String, String]): Either[Throwable, HttpSinkConfig] = {
    val connectConfig = new AbstractConfig(HttpSinkConfigDef.config, configs.asJava)
    for {
      method <- HttpMethod.withNameInsensitiveEither(connectConfig.getString(HttpSinkConfigDef.HttpMethodProp))
        .leftMap(e =>
          new IllegalArgumentException(
            s"Invalid HTTP method. Supported methods are: ${HttpMethod.values.mkString(", ")}",
            e,
          ),
        )
      endpoint <- extractEndpoint(connectConfig.getString(HttpSinkConfigDef.HttpEndpointProp))
      content   = connectConfig.getString(HttpSinkConfigDef.HttpRequestContentProp)
      headers  <- extractHeaders(connectConfig.getList(HttpSinkConfigDef.HttpRequestHeadersProp).asScala.toList)
      auth <- Authentication.from(
        connectConfig,
        AuthenticationKeys(BasicAuthenticationUsernameProp, BasicAuthenticationPasswordProp, AuthenticationTypeProp),
      )
      ssl  <- CyclopsToScalaEither.convertToScalaEither(StoresInfo.fromConfig(connectConfig))
      batch = BatchConfig.from(connectConfig)
      nullPayloadHandler <- NullPayloadHandler(
        connectConfig.getString(HttpSinkConfigDef.NullPayloadHandler),
        connectConfig.getString(HttpSinkConfigDef.CustomNullPayloadHandler),
      )
      errorThreshold      = connectConfig.getInt(HttpSinkConfigDef.ErrorThresholdProp)
      uploadSyncPeriod    = connectConfig.getInt(HttpSinkConfigDef.UploadSyncPeriodProp)
      retries            <- RetriesConfig.fromConfig(connectConfig)
      connectionTimeoutMs = connectConfig.getInt(HttpSinkConfigDef.ConnectionTimeoutMsProp)
      timeout             = TimeoutConfig(connectionTimeoutMs)
      jsonTidy            = connectConfig.getBoolean(HttpSinkConfigDef.JsonTidyProp)
      errorReportingController = createAndStartController(
        ErrorReportingController.fromAbstractConfig[HttpFailureConnectorSpecificRecordData](
          (reportingMessagesConfig: ReportingMessagesConfig) =>
            new RecordConverter(
              reportingMessagesConfig,
              HttpFailureSpecificHeaderRecordConverter.apply,
            ),
          connectConfig,
        ),
      )
      successReportingController = createAndStartController(
        SuccessReportingController.fromAbstractConfig[HttpSuccessConnectorSpecificRecordData](
          (reportingMessagesConfig: ReportingMessagesConfig) =>
            new RecordConverter(
              reportingMessagesConfig,
              HttpSuccessSpecificHeaderRecordConverter.apply,
            ),
          connectConfig,
        ),
      )

      maxQueueSize = connectConfig.getInt(HttpSinkConfigDef.MaxQueueSizeProp)
      maxQueueOfferTimeout = FiniteDuration(
        connectConfig.getLong(HttpSinkConfigDef.MaxQueueOfferTimeoutProp),
        scala.concurrent.duration.MILLISECONDS,
      )
    } yield HttpSinkConfig(
      method,
      endpoint,
      content,
      auth,
      headers,
      ssl,
      batch,
      nullPayloadHandler,
      errorThreshold,
      uploadSyncPeriod,
      retries,
      timeout,
      jsonTidy,
      errorReportingController,
      successReportingController,
      maxQueueSize,
      maxQueueOfferTimeout,
    )
  }

  def extractEndpoint(endpoint: String): Either[Throwable, String] = {
    def isValidUrl(url: String): Boolean = Try(new URL(url)).isSuccess

    endpoint.trim match {
      case ""                              => Left(new IllegalArgumentException("Endpoint cannot be empty"))
      case trimmed if !isValidUrl(trimmed) => Left(new MalformedURLException(s"Invalid URL: $trimmed"))
      case validEndpoint                   => Right(validEndpoint)
    }
  }

  /**
   * Extracts the headers, and if Content-Type is not set, it will be added to the headers
   * @param headers - the headers string "key1:value1;key2:value2"
   * @return
   */
  def extractHeaders(headers: List[String]): Either[Throwable, List[(String, String)]] = {
    def parseHeader(header: String): Either[Throwable, (String, String)] =
      header.split(":", 2) match {
        case Array(key, value) => Right((key.trim, value.trim))
        case _                 => Left(new IllegalArgumentException(s"Invalid header format: $header"))
      }

    val parsedHeadersResult = headers.foldLeft[Either[Throwable, List[(String, String)]]](Right(List.empty)) {
      (acc, header) =>
        for {
          accList      <- acc
          parsedHeader <- parseHeader(header)
        } yield parsedHeader :: accList
    }

    parsedHeadersResult.map { parsedHeaders =>
      if (parsedHeaders.exists(_._1.equalsIgnoreCase("Content-Type"))) {
        parsedHeaders
      } else {
        ("Content-Type", "application/json") :: parsedHeaders
      }
    }
  }

  private def createAndStartController[C <: ConnectorSpecificRecordData](
    controller: ReportingController[C],
  ): ReportingController[C] = {
    controller.start()
    controller
  }

}
