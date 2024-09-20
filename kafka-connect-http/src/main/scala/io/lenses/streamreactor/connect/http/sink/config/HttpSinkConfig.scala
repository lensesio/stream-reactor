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
package io.lenses.streamreactor.connect.http.sink.config

import cats.implicits.toBifunctorOps
import cats.implicits.toTraverseOps
import io.lenses.streamreactor.common.security.StoresInfo
import io.lenses.streamreactor.common.utils.CyclopsToScalaEither
import io.lenses.streamreactor.connect.cloud.common.sink.commit._
import io.lenses.streamreactor.connect.http.sink.client.Authentication
import io.lenses.streamreactor.connect.http.sink.client.AuthenticationKeys
import io.lenses.streamreactor.connect.http.sink.client.HttpMethod
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef.AuthenticationTypeProp
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef.BasicAuthenticationPasswordProp
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef.BasicAuthenticationUsernameProp
import io.lenses.streamreactor.connect.reporting.ReportingController
import io.lenses.streamreactor.connect.reporting.ReportingController.ErrorReportingController
import io.lenses.streamreactor.connect.reporting.ReportingController.SuccessReportingController
import org.apache.kafka.common.config.AbstractConfig

import java.net.MalformedURLException
import java.net.URL
import java.time.Clock
import java.time.Duration
import scala.jdk.CollectionConverters._
import scala.util.Try

case class BatchConfig(
  batchCount:   Option[Long],
  batchSize:    Option[Long],
  timeInterval: Option[Long],
) {
  def toCommitPolicy: CommitPolicy = {
    val conditions: Seq[CommitPolicyCondition] = Seq(
      batchCount.map(Count),
      batchSize.map(FileSize),
      timeInterval.map(inter => Interval(Duration.ofSeconds(inter), Clock.systemDefaultZone())),
    ).flatten

    CommitPolicy(conditions: _*)
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

case class RetriesConfig(
  maxRetries:    Int,
  maxTimeoutMs:  Int,
  onStatusCodes: List[Int],
)

case class HttpSinkConfig(
  method:                     HttpMethod,
  endpoint:                   String,
  content:                    String,
  authentication:             Authentication,
  headers:                    List[(String, String)],
  ssl:                        StoresInfo,
  batch:                      BatchConfig,
  errorThreshold:             Int,
  uploadSyncPeriod:           Int,
  retries:                    RetriesConfig,
  timeout:                    TimeoutConfig,
  errorReportingController:   ReportingController,
  successReportingController: ReportingController,
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
      ssl             <- CyclopsToScalaEither.convertToScalaEither(StoresInfo.fromConfig(connectConfig))
      batch            = BatchConfig.from(connectConfig)
      errorThreshold   = connectConfig.getInt(HttpSinkConfigDef.ErrorThresholdProp)
      uploadSyncPeriod = connectConfig.getInt(HttpSinkConfigDef.UploadSyncPeriodProp)
      maxRetries       = connectConfig.getInt(HttpSinkConfigDef.RetriesMaxRetriesProp)
      maxTimeoutMs     = connectConfig.getInt(HttpSinkConfigDef.RetriesMaxTimeoutMsProp)
      onStatusCodes <- Option(connectConfig.getList(HttpSinkConfigDef.RetriesOnStatusCodesProp))
        .map(_.asScala.toList).filter(_.nonEmpty)
        .getOrElse(Nil)
        .traverse(v => Try(v.toInt).toEither.leftMap(e => new IllegalArgumentException(s"Invalid status code: $v", e)))
      retries                    = RetriesConfig(maxRetries, maxTimeoutMs, onStatusCodes)
      connectionTimeoutMs        = connectConfig.getInt(HttpSinkConfigDef.ConnectionTimeoutMsProp)
      timeout                    = TimeoutConfig(connectionTimeoutMs)
      errorReportingController   = createAndStartController(new ErrorReportingController(connectConfig))
      successReportingController = createAndStartController(new SuccessReportingController(connectConfig))
    } yield HttpSinkConfig(
      method,
      endpoint,
      content,
      auth,
      headers,
      ssl,
      batch,
      errorThreshold,
      uploadSyncPeriod,
      retries,
      timeout,
      errorReportingController,
      successReportingController,
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

  private def createAndStartController(controller: ReportingController): ReportingController = {
    controller.start()
    controller
  }

}
