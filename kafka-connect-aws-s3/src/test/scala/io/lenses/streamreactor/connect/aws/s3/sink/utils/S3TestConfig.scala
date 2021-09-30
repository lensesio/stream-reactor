
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
import io.lenses.streamreactor.connect.aws.s3.model.location.{LocalPathLocation, LocalRootLocation}
import io.lenses.streamreactor.connect.aws.s3.sink.ThrowableEither._
import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3ProxyContext.TestBucket
import io.lenses.streamreactor.connect.aws.s3.storage.{JCloudsStorageInterface, StorageInterface}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, Delete, DeleteObjectsRequest, ObjectIdentifier}

import java.nio.file.Files
import scala.util.Try



trait S3TestConfig extends AnyFlatSpec with BeforeAndAfter with BeforeAndAfterAll with Matchers with LazyLogging {

  var localRoot: LocalRootLocation = _
  var localPath : LocalPathLocation = _

  def cleanUpEnabled : Boolean = true

  def setUpTestData(): Unit = {}

  protected val proxyContext: S3ProxyContext = new S3ProxyContext()

  private val s3Config = S3Config(
    region = Some("us-east-1"),
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
    storageInterface <- Try {new JCloudsStorageInterface("test", jCloudsAuthResource)}.toEither
  } yield (storageInterface, awsAuthResource)

  implicit val storageInterface: StorageInterface = storageInterfaceS3ClientEither.toThrowable("testSink")._1
  private val s3Client: S3Client = storageInterfaceS3ClientEither.toThrowable("testSink")._2

  val BucketName: String = S3ProxyContext.TestBucket


  before {
    if (cleanUpEnabled) {
      clearTestBucket()
      setUpTestData()
    }

    localRoot = LocalRootLocation(Files.createTempDirectory("blah").toAbsolutePath.toString)
    localPath = LocalPathLocation(Files.createTempFile("blah", "blah").toAbsolutePath.toString, createFile = false)
  }

  override protected def beforeAll(): Unit = {

    super.beforeAll()

    logger.debug("Starting proxy")
    proxyContext.startProxy

    logger.debug("Creating test bucket")
    createTestBucket() match {
      case Left(err) => logger.error("Error creating test bucket", err)
      case Right(_) =>
    }
    clearTestBucket()
    setUpTestData()
  }

  override protected def afterAll(): Unit = {

    super.afterAll()

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
    // It is fine if it already exists
    Try(s3Client.createBucket(CreateBucketRequest.builder().bucket(TestBucket).build())).toEither.right.map(_ => ())
  }
}
