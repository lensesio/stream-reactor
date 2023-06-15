package io.lenses.streamreactor.connect

import _root_.io.circe.parser._
import _root_.io.lenses.streamreactor.connect.model.Order
import _root_.io.lenses.streamreactor.connect.testcontainers.S3Authentication
import cats.effect.IO
import cats.effect.Resource
import com.jayway.jsonpath.internal.Utils.unescape
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.scalatest.EitherValues
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model._

import java.io.InputStream
import java.net.URI
import scala.util.Try

object S3Utils extends EitherValues with LazyLogging {

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
      IO.fromTry {
        Try {
          s3Client.deleteBucket(
            DeleteBucketRequest
              .builder()
              .bucket(bucketName)
              .build(),
          )
          ()
        }
      }.recoverWith {
        case ex: Throwable =>
          logger.error("Error deleting bucket", ex)
          IO.unit
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
