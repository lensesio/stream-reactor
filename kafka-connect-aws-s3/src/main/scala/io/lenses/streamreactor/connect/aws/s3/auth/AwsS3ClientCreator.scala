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
package io.lenses.streamreactor.connect.aws.s3.auth

import cats.implicits.catsSyntaxEitherId
import cats.implicits.toBifunctorOps
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.S3ConnectionConfig
import io.lenses.streamreactor.connect.cloud.common.auth.ClientCreator
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration

import java.net.URI
import java.time.Duration
import scala.util.Try

object AwsS3ClientCreator extends ClientCreator[S3ConnectionConfig, S3Client] {

  private val missingCredentialsError =
    "Configured to use credentials however one or both of `AWS_ACCESS_KEY` or `AWS_SECRET_KEY` are missing."

  private val defaultCredentialsProvider: AwsCredentialsProvider = DefaultCredentialsProvider.create()

  def make(config: S3ConnectionConfig): Either[Throwable, S3Client] =
    for {
      retryPolicy <- Try {
        RetryPolicy
          .builder()
          .numRetries(config.httpRetryConfig.numberOfRetries)
          .backoffStrategy(
            FixedDelayBackoffStrategy.create(Duration.ofMillis(config.httpRetryConfig.errorRetryInterval)),
          )
          .build()
      }.toEither

      overrideConfig <- Try(ClientOverrideConfiguration.builder().retryPolicy(retryPolicy).build()).toEither

      s3Config <- Try {
        S3Configuration
          .builder
          .pathStyleAccessEnabled(config.enableVirtualHostBuckets)
          .build
      }.toEither

      httpClient <- Try {
        val apacheHttpClientBuilder = ApacheHttpClient.builder()
        config.timeouts.socketTimeout.foreach(t => apacheHttpClientBuilder.socketTimeout(Duration.ofMillis(t.toLong)))
        config.timeouts.connectionTimeout.foreach(t => apacheHttpClientBuilder.connectionTimeout(Duration.ofMillis(t)))
        config.connectionPoolConfig.foreach(t => apacheHttpClientBuilder.maxConnections(t.maxConnections))
        apacheHttpClientBuilder.build()
      }.toEither
      s3Client <- credentialsProvider(config).leftMap(new IllegalArgumentException(_)).flatMap { credsProv =>
        Try(
          S3Client
            .builder()
            .overrideConfiguration(overrideConfig)
            .serviceConfiguration(s3Config)
            .credentialsProvider(credsProv)
            .httpClient(httpClient),
        ).toEither

      }.map { builder =>
        config
          .region
          .fold(builder)(reg => builder.region(Region.of(reg)))
      }.map { builder =>
        config.customEndpoint.fold(builder)(cE => builder.endpointOverride(URI.create(cE)))
      }.flatMap { builder =>
        Try(builder.build()).toEither
      }
    } yield s3Client

  private def credentialsFromConfig(awsConfig: S3ConnectionConfig): Either[String, AwsCredentialsProvider] =
    awsConfig.accessKey.zip(awsConfig.secretKey) match {
      case Some((access, secret)) =>
        new AwsCredentialsProvider {
          override def resolveCredentials(): AwsCredentials = AwsBasicCredentials.create(access, secret)
        }.asRight
      case None => missingCredentialsError.asLeft
    }

  private def credentialsProvider(config: S3ConnectionConfig): Either[String, AwsCredentialsProvider] =
    config.authMode match {
      case AuthMode.Credentials => credentialsFromConfig(config)
      case AuthMode.Default     => defaultCredentialsProvider.asRight[String]
    }

}
