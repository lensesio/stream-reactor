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

import io.lenses.streamreactor.connect.aws.s3.config.{AuthMode, S3Config}
import org.jclouds.blobstore.BlobStoreContext
import org.jclouds.providers.ProviderMetadata
import org.jclouds.rest.internal.ApiContextImpl
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.{AwsCredentials, AwsCredentialsProvider}

class JCloudsS3ContextCreatorTest extends AnyFlatSpec with MockitoSugar with Matchers with BeforeAndAfter {

  private val AWS_ACCESS_KEY_ID = "awsAccessKeyId"

  private val awsCredentials = mock[AwsCredentials]
  private val credentialsProvider = mock[AwsCredentialsProvider]
  private val credentialsProviderFn = mock[() => AwsCredentialsProvider]

  private val target = new JCloudsS3ContextCreator(credentialsProviderFn)

  before {
    reset(awsCredentials, credentialsProvider, credentialsProviderFn)
  }

  "fromConfig" should "use default credentials provider when auth mode is 'Default'" in {

    when(awsCredentials.accessKeyId()).thenReturn(AWS_ACCESS_KEY_ID)
    when(credentialsProvider.resolveCredentials()).thenReturn(awsCredentials)
    when(credentialsProviderFn.apply()).thenReturn(credentialsProvider)

    val blobStoreContext = target.fromConfig(
      S3Config(None, None, None, AuthMode.Default)
    )

    getIdentityFromContext(blobStoreContext) should be(AWS_ACCESS_KEY_ID)

  }

  "fromConfig" should "use supplied credentials when auth mode is 'Credentials'" in {

    val blobStoreContext = target.fromConfig(
      S3Config(
        None,
        Some("CONFIGURED_ACCESS_KEY_ID"),
        Some("CONFIGURED_SECRET"),
        AuthMode.Credentials
      )
    )

    getIdentityFromContext(blobStoreContext) should be("CONFIGURED_ACCESS_KEY_ID")

    verifyZeroInteractions(credentialsProviderFn, credentialsProvider, awsCredentials)
  }

  "fromConfig" should "fail when auth mode is 'Credentials' but no credentials are supplied" in {

    val blobStoreContext = target.fromConfig(
      S3Config(None, None, None, AuthMode.Credentials)
    )

    intercept[IllegalArgumentException] {
      getIdentityFromContext(blobStoreContext)
    }.getMessage should be("Configured to use credentials however one or both of `AWS_ACCESS_KEY` or `AWS_SECRET_KEY` are missing.")

    verifyZeroInteractions(credentialsProviderFn, credentialsProvider, awsCredentials)

  }

  "fromConfig" should "fail when no credentials are found on the default provider chain" in {

    when(credentialsProvider.resolveCredentials()).thenReturn(null)
    when(credentialsProviderFn.apply()).thenReturn(credentialsProvider)

    val blobStoreContext = target.fromConfig(S3Config(None, None, None, AuthMode.Default))

    intercept[IllegalStateException] {
      getIdentityFromContext(blobStoreContext)
    }.getMessage should be("No credentials found on default provider chain.")

    verify(credentialsProviderFn, times(1)).apply()
    verify(credentialsProvider, times(1)).resolveCredentials()
    verifyZeroInteractions(awsCredentials)
  }

  "fromConfig" should "fail when empty string credentials are provided" in {

    val blobStoreContext = target.fromConfig(
      S3Config(None, Some(""), Some(""), AuthMode.Credentials)
    )

    intercept[IllegalArgumentException] {
      getIdentityFromContext(blobStoreContext)
    }.getMessage should be("Configured to use credentials however one or both of `AWS_ACCESS_KEY` or `AWS_SECRET_KEY` are missing.")

    verifyZeroInteractions(credentialsProviderFn, credentialsProvider, awsCredentials)

  }

  private def getIdentityFromContext(blobStoreContext: BlobStoreContext): String = {
    val apiContextImpl: ApiContextImpl[ProviderMetadata] = blobStoreContext.unwrap()
    apiContextImpl.getIdentity
  }

}
