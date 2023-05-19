/*
 * Copyright 2017-2023 Lenses.io Ltd
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
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.S3Config
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
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class AwsS3ClientCreator(config: S3Config) {

  private val missingCredentialsError =
    "Configured to use credentials however one or both of `AWS_ACCESS_KEY` or `AWS_SECRET_KEY` are missing."

  private val defaultCredentialsProvider: AwsCredentialsProvider = DefaultCredentialsProvider.create()

  def createS3Client(): Either[String, S3Client] =
    Try {

      val retryPolicy = RetryPolicy
        .builder()
        .numRetries(config.httpRetryConfig.numberOfRetries)
        .backoffStrategy(
          FixedDelayBackoffStrategy.create(Duration.ofMillis(config.httpRetryConfig.errorRetryInterval)),
        )
        .build()

      val overrideConfig = ClientOverrideConfiguration.builder().retryPolicy(retryPolicy).build()

      val s3Config = S3Configuration
        .builder
        .pathStyleAccessEnabled(config.enableVirtualHostBuckets)
        .build

      val apacheHttpClientBuilder = ApacheHttpClient.builder()
      config.timeouts.socketTimeout.foreach(t => apacheHttpClientBuilder.socketTimeout(Duration.ofMillis(t.toLong)))
      config.timeouts.connectionTimeout.foreach(t =>
        apacheHttpClientBuilder.connectionTimeout(Duration.ofMillis(t.toLong)),
      )
      config.connectionPoolConfig.foreach(t => apacheHttpClientBuilder.maxConnections(t.maxConnections))

      val s3ClientBuilder = credentialsProvider match {
        case Left(err) => return err.asLeft
        case Right(credsProv) => S3Client
            .builder()
            .overrideConfiguration(overrideConfig)
            .serviceConfiguration(s3Config)
            .credentialsProvider(credsProv)
            .httpClient(apacheHttpClientBuilder.build())

      }

      config
        .region
        .foreach(reg => s3ClientBuilder.region(Region.of(reg)))

      config
        .customEndpoint
        .foreach(cE => s3ClientBuilder.endpointOverride(URI.create(cE)))

      s3ClientBuilder
        .build()

    } match {
      case Failure(exception) => exception.getMessage.asLeft
      case Success(value)     => value.asRight
    }

  private def credentialsFromConfig(awsConfig: S3Config): Either[String, AwsCredentialsProvider] =
    awsConfig.accessKey.zip(awsConfig.secretKey).headOption match {
      case Some((access, secret)) =>
        new AwsCredentialsProvider {
          override def resolveCredentials(): AwsCredentials = AwsBasicCredentials.create(access, secret)
        }.asRight
      case None => missingCredentialsError.asLeft
    }

  private def credentialsProvider: Either[String, AwsCredentialsProvider] =
    config.authMode match {
      case AuthMode.Credentials => credentialsFromConfig(config)
      case AuthMode.Default     => defaultCredentialsProvider.asRight[String]
    }

}
