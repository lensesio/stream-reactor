
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

import java.util.Properties

import io.lenses.streamreactor.connect.aws.s3.config.{AuthMode, S3Config}
import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext


object AwsContextCreator {

  def fromConfig(awsConfig: S3Config): BlobStoreContext = {

    val (access, secret) = getAuthModeFn(awsConfig)

    val contextBuilder = ContextBuilder
      .newBuilder("aws-s3")
      .credentials(access, secret)

    awsConfig.customEndpoint.foreach(contextBuilder.endpoint)

    if (awsConfig.enableVirtualHostBuckets) {
      contextBuilder.overrides(createOverride())
    }

    contextBuilder.buildView(classOf[BlobStoreContext])

  }

  private def getAuthModeFn(awsConfig: S3Config): (String, String) =

    awsConfig.authMode match {
      case AuthMode.Credentials => credentialsConfigFn(awsConfig: S3Config)
      case _ => throwErrorConfigFn(awsConfig.authMode)
    }

  private def credentialsConfigFn = (awsConfig: S3Config) => (
    awsConfig.accessKey,
    awsConfig.secretKey
  )

  private def throwErrorConfigFn(authMode: AuthMode) = throw new NotImplementedError(s"The auth mode $authMode is not currently supported")

  private def createOverride() = {
    val overrides = new Properties()
    overrides.put(org.jclouds.s3.reference.S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false")
    overrides
  }

  def close(blobStoreContext: BlobStoreContext): Unit = {
    blobStoreContext.close()
  }


}
