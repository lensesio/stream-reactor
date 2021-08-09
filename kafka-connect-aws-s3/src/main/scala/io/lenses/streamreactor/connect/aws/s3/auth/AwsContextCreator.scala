
/*
 * Copyright 2020 Lenses.io
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

import com.amazonaws.auth.{AWSCredentialsProvider, AWSSessionCredentials, DefaultAWSCredentialsProviderChain}
import com.google.common.base.Supplier
import io.lenses.streamreactor.connect.aws.s3.config.{AuthMode, S3Config}
import org.jclouds.ContextBuilder
import org.jclouds.aws.domain.SessionCredentials
import org.jclouds.blobstore.BlobStoreContext
import org.jclouds.domain.Credentials

import java.util.Properties

object AwsContextCreator {

  def DefaultCredentialsFn: () => AWSCredentialsProvider = {
    () => new DefaultAWSCredentialsProviderChain()
  }

}

class AwsContextCreator(credentialsProviderFn: () => AWSCredentialsProvider) {

  private val missingCredentialsError = "Configured to use credentials however one or both of `AWS_ACCESS_KEY` or `AWS_SECRET_KEY` are missing."

  def fromConfig(awsConfig: S3Config): BlobStoreContext = {

    val contextBuilder = ContextBuilder
      .newBuilder("aws-s3")
      .credentialsSupplier(credentialsSupplier(awsConfig))

    awsConfig.customEndpoint.foreach(contextBuilder.endpoint)

    contextBuilder.overrides(createOverride(awsConfig))

    contextBuilder.buildView(classOf[BlobStoreContext])

  }

  private def credentialsSupplier(awsConfig: S3Config): Supplier[Credentials] = {
    awsConfig.authMode match {
      case AuthMode.Credentials => credentialsFromConfig(awsConfig: S3Config)
      case _ => credentialsFromDefaultChain()
    }
  }

  private def credentialsFromConfig(awsConfig: S3Config): Supplier[Credentials] = () => new Credentials(
    awsConfig.accessKey.filter(_.trim.nonEmpty).getOrElse(throw new IllegalArgumentException(missingCredentialsError)),
    awsConfig.secretKey.filter(_.trim.nonEmpty).getOrElse(throw new IllegalArgumentException(missingCredentialsError))
  )

  private def credentialsFromDefaultChain(): Supplier[Credentials] = () => {
    val credentialsProvider = credentialsProviderFn()
    val credentials = Option(credentialsProvider.getCredentials)
      .getOrElse(throw new IllegalStateException("No credentials found on default provider chain."))
    credentials match {
      case credentials: AWSSessionCredentials => SessionCredentials.builder().accessKeyId(credentials.getAWSAccessKeyId).secretAccessKey(credentials.getAWSSecretKey).sessionToken(credentials.getSessionToken).build()
      case _ => new Credentials(credentials.getAWSAccessKeyId, credentials.getAWSSecretKey)
    }
  }

  private def createOverride(awsConfig: S3Config) = {
    val overrides = new Properties()
    if (awsConfig.enableVirtualHostBuckets) {
      overrides.put(org.jclouds.s3.reference.S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false")
    }
    overrides.put(org.jclouds.Constants.PROPERTY_MAX_RETRIES, awsConfig.httpRetryConfig.numberOfRetries.toString)
    overrides.put(org.jclouds.Constants.PROPERTY_RETRY_DELAY_START, awsConfig.httpRetryConfig.errorRetryInterval.toString)
    overrides
  }

  def close(blobStoreContext: BlobStoreContext): Unit = {
    blobStoreContext.close()
  }

}
