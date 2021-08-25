/*
 * Copyright 2021 Lenses.io
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
import io.lenses.streamreactor.connect.aws.s3.config.{AuthMode, S3Config}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsCredentials, AwsCredentialsProvider, DefaultCredentialsProvider}
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.net.URI
import scala.util.{Failure, Success, Try}

class AwsS3ClientCreator(config: S3Config) {

  private val missingCredentialsError = "Configured to use credentials however one or both of `AWS_ACCESS_KEY` or `AWS_SECRET_KEY` are missing."

  val defaultCredentialsProvider: AwsCredentialsProvider = DefaultCredentialsProvider.create()

  def createS3Client() : Either[String, S3Client] = {
    Try {

      val s3Config = S3Configuration
        .builder
        .pathStyleAccessEnabled(config.enableVirtualHostBuckets)
        .build

      val s3ClientBuilder = credentialsProvider match {
        case Left(err) => return err.asLeft
        case Right(credsProv) => S3Client
          .builder()
          .serviceConfiguration(s3Config)
          .credentialsProvider(credsProv)
      }

      config
        .customEndpoint
        .foreach(
          cE => s3ClientBuilder.endpointOverride(URI.create(cE))
        )

      s3ClientBuilder
        .build()


    } match {
      case Failure(exception) => exception.getMessage.asLeft
      case Success(value) => value.asRight
    }
  }

  private def credentialsFromConfig(awsConfig: S3Config): Either[String, AwsCredentialsProvider] = {
    awsConfig.accessKey.zip(awsConfig.secretKey).headOption match {
      case Some((access, secret)) =>
        new AwsCredentialsProvider {
            override def resolveCredentials(): AwsCredentials = AwsBasicCredentials.create(access, secret)
          }.asRight
      case None => missingCredentialsError.asLeft
    }
  }

  private def credentialsProvider: Either[String, AwsCredentialsProvider] = {
    config.authMode match {
      case AuthMode.Credentials => credentialsFromConfig(config)
      case AuthMode.Default => defaultCredentialsProvider.asRight[String]
    }
  }

}
