
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

import com.typesafe.scalalogging.LazyLogging
import org.eclipse.jetty.util.component.AbstractLifeCycle
import org.gaul.s3proxy.S3Proxy
import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, Delete, DeleteObjectsRequest, DeleteObjectsResponse, ListObjectsRequest, ListObjectsV2Request, ObjectIdentifier}
import software.amazon.awssdk.services.s3.{S3Client, S3ClientBuilder}

import java.net.URI
import java.util.Properties
import scala.collection.JavaConverters._
import scala.util.Try

object S3ProxyContext {

  val Port: Int = 8099
  val Identity: String = "identity"
  val Credential: String = "credential"
  val Uri: String = "http://127.0.0.1:" + Port

  val TestBucket: String = "employees"
}

class S3ProxyContext extends LazyLogging {

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

    proxy.start()
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

  def createTestBucket: Try[DeleteObjectsResponse] = {

    // create bucket using the proper client

    val s3Client = S3Client
      .builder()
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(Identity, Credential)))
      .endpointOverride(URI.create(Uri))
      .region(Region.US_EAST_1)
      .build()


    // I don't care if it already exists
    Try(s3Client.createBucket(CreateBucketRequest.builder().bucket(TestBucket).build()))

    Try(clearTestBucket(s3Client))

  }

  def clearTestBucket(s3Client: S3Client): DeleteObjectsResponse = {
    val toDelete = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(TestBucket).build()).contents().asScala
    val toDeleteArray = toDelete.map(delMe => ObjectIdentifier.builder().key(delMe.key()).build())
    val delete = Delete.builder().objects(toDeleteArray: _*).build
    s3Client.deleteObjects(DeleteObjectsRequest.builder().bucket(TestBucket).delete(delete).build())
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
