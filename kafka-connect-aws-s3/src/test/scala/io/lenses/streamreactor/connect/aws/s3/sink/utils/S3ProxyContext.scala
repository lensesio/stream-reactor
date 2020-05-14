
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

package io.lenses.streamreactor.connect.aws.s3.sink.utils

import java.net.URI
import java.util.Properties

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.{DeleteObjectsRequest, DeleteObjectsResult}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.eclipse.jetty.util.component.AbstractLifeCycle
import org.gaul.s3proxy.S3Proxy
import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext

import scala.collection.JavaConverters._
import scala.util.Try

object S3ProxyContext {

  val Port: Int = 8099
  val Identity: String = "identity"
  val Credential: String = "credential"
  val Uri: String = "http://127.0.0.1:" + Port

  val TestBucket: String = "employees"
}

class S3ProxyContext {

  import S3ProxyContext._

  private val proxy: S3Proxy = createProxy

  def createProxy: S3Proxy = {

    val properties = new Properties()
    properties.setProperty("jclouds.filesystem.basedir", "/tmp/blobstore")

    val context: BlobStoreContext = ContextBuilder
      .newBuilder("filesystem")
      .credentials(Identity, Credential)
      .overrides(properties)
      .build(classOf[BlobStoreContext])

    S3Proxy.builder
      .blobStore(context.getBlobStore)
      .endpoint(URI.create(Uri))
      .build

  }

  def startProxy: S3Proxy = {

    proxy.start
    while ( {
      !proxy.getState.equals(AbstractLifeCycle.STARTED)
    }) Thread.sleep(1)

    proxy
  }

  def stopProxy: S3Proxy = {
    proxy.stop()
    while ( {
      !proxy.getState.equals(AbstractLifeCycle.STOPPED)
    }) Thread.sleep(1)

    proxy
  }

  def createTestBucket: Try[DeleteObjectsResult] = {

    // create bucket using the proper client

    val s3Client = AmazonS3ClientBuilder
      .standard
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(Identity, Credential)))
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(Uri, Regions.US_EAST_1.getName))
      .build


    // I don't care if it already exists
    Try(s3Client.createBucket(TestBucket))

    Try(clearTestBucket(s3Client))

  }

  def clearTestBucket(s3Client: AmazonS3): DeleteObjectsResult = {
    val toDelete = s3Client.listObjects(TestBucket).getObjectSummaries.asScala
    val toDeleteArray = toDelete.map(_.getKey).toArray[String]
    s3Client.deleteObjects(new DeleteObjectsRequest(TestBucket).withKeys(toDeleteArray: _*))
  }

  def createBlobStoreContext: BlobStoreContext = {

    val overrides = new Properties()
    overrides.put(org.jclouds.s3.reference.S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, "false")

    ContextBuilder
      .newBuilder("aws-s3")
      .credentials(S3ProxyContext.Identity, S3ProxyContext.Credential)
      .overrides(overrides)
      .endpoint(S3ProxyContext.Uri)
      .buildView(classOf[BlobStoreContext])

  }

}
