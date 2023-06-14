package io.lenses.streamreactor.connect

import com.jayway.jsonpath.internal.Utils.unescape
import _root_.io.circe.parser._
import _root_.io.lenses.streamreactor.connect.model.Order
import _root_.io.lenses.streamreactor.connect.testcontainers.S3Authentication
import cats.effect.IO
import cats.effect.Resource
import org.apache.commons.io.IOUtils
import org.scalatest.EitherValues
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.CreateBucketResponse
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration

import java.io.InputStream
import java.net.URI
import scala.util.Try

object S3Utils extends EitherValues {

  def createBucket(s3Client: S3Client, bucketName: String): Resource[IO, CreateBucketResponse] =
    Resource.make(
      IO {
        s3Client.createBucket(
          CreateBucketRequest
            .builder()
            .bucket(bucketName)
            .createBucketConfiguration(
              CreateBucketConfiguration.builder()
                .locationConstraint(BucketLocationConstraint.EU_WEST_1)
                .build(),
            )
            .build(),
        )
      },
    ) { _ =>
      IO {
        Try(
          s3Client.deleteBucket(
            DeleteBucketRequest
              .builder()
              .bucket(bucketName)
              .build(),
          ),
        )
        ()
      }
    }

  def createS3ClientResource(identity: S3Authentication, endpoint: URI): Resource[IO, S3Client] =
    Resource.fromAutoCloseable(
      IO(
        S3Client
          .builder()
          .endpointOverride(endpoint)
          .region(Region.of("custom"))
          .credentialsProvider(() => AwsBasicCredentials.create(identity.identity, identity.credential))
          .serviceConfiguration(S3Configuration
            .builder
            .pathStyleAccessEnabled(true)
            .build)
          .build(),
      ),
    )

  def readKeyToOrder(s3Client: S3Client, bucketName: String, key: String): Order = {
    val is: InputStream = s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build())
    val fileBytes = IOUtils.toByteArray(is)
    decodeEscapedJson(new String(fileBytes))
  }

  def decodeEscapedJson(input: String): Order = {
    val a1 = unescape(input.trim)
    // remove the quotes from around the json
    val a2 = a1.substring(1, a1.length - 1)
    decode[Order](a2).value
  }

}
