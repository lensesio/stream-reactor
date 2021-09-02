
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
import io.lenses.streamreactor.connect.aws.s3.auth.AuthResources
import io.lenses.streamreactor.connect.aws.s3.config.{AuthMode, S3Config}
import io.lenses.streamreactor.connect.aws.s3.sink.ThrowableEither._
import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3ProxyContext.{Credential, Identity, TestBucket}
import io.lenses.streamreactor.connect.aws.s3.storage.S3StorageInterface
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, Delete, DeleteObjectsRequest, ObjectIdentifier}

import java.net.URI
import scala.util.Try



trait S3TestConfig extends AnyFlatSpec with BeforeAndAfter with Matchers with LazyLogging {

  protected val proxyContext: S3ProxyContext = new S3ProxyContext()

  private val s3Config = S3Config(
    accessKey = Some(S3ProxyContext.Identity),
    secretKey = Some(S3ProxyContext.Credential),
    authMode = AuthMode.Credentials,
    customEndpoint = Some(S3ProxyContext.Uri),
    enableVirtualHostBuckets = true,
  )
  private val storageInterfaceS3ClientEither = for {
    authResource <- Try {new AuthResources(s3Config)}.toEither
    awsAuthResource <- authResource.aws
    jCloudsAuthResource <- authResource.jClouds
    storageInterface <- Try {new S3StorageInterface("test", awsAuthResource, jCloudsAuthResource)}.toEither
  } yield (storageInterface, awsAuthResource)

  implicit val storageInterface: S3StorageInterface = storageInterfaceS3ClientEither.toThrowable("testSink")._1
  private val s3Client: S3Client = storageInterfaceS3ClientEither.toThrowable("testSink")._2

  val BucketName: String = S3ProxyContext.TestBucket
  private var bucketExists = false

  before {
    logger.debug("Starting proxy")
    proxyContext.startProxy

    if (!bucketExists) {
      logger.debug("Creating test bucket")
      createTestBucket()// should be ('right)
      bucketExists = true
    }

    clearTestBucket()

  }

  after {
    proxyContext.stopProxy
  }

  val helper = new RemoteFileTestHelper()

  private def clearTestBucket(): Either[Throwable, Unit] = {
    Try {

      val toDeleteArray = helper
        .listBucketPath(BucketName, "")
        .map(ObjectIdentifier.builder().key(_).build())
      val delete = Delete.builder().objects(toDeleteArray: _*).build
      s3Client.deleteObjects(DeleteObjectsRequest.builder().bucket(TestBucket).delete(delete).build())

    }.toEither.right.map(_ => ())
  }

  private def createTestBucket(): Either[Throwable, Unit] = {

    // Create the bucket using the AWS client.
    // The only difference between this and using the s3Client already available is that the region is set.

    val s3Client = S3Client
      .builder()
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(Identity, Credential)))
      .endpointOverride(URI.create(S3ProxyContext.Uri))
      .region(Region.US_EAST_1)
      .build()


    // I don't care if it already exists
    Try(s3Client.createBucket(CreateBucketRequest.builder().bucket(TestBucket).build())).toEither.right.map(_ => ())
  }
}
