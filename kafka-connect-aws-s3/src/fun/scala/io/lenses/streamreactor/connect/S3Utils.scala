package io.lenses.streamreactor.connect

import com.jayway.jsonpath.internal.Utils.unescape
import io.circe.parser._
import io.lenses.streamreactor.connect.model.Order
import io.lenses.streamreactor.connect.testcontainers.S3Authentication
import org.apache.commons.io.IOUtils
import org.scalatest.EitherValues
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.{BucketLocationConstraint, CreateBucketConfiguration, CreateBucketRequest, GetObjectRequest}
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.io.InputStream
import java.net.URI

object S3Utils extends EitherValues {

  def createBucket(s3Client: S3Client, bucketName: String) = {
    s3Client.createBucket(
      CreateBucketRequest
        .builder()
        .bucket(bucketName)
        .createBucketConfiguration(
          CreateBucketConfiguration.builder()
            .locationConstraint(BucketLocationConstraint.EU_WEST_1)
            .build()
        )
        .build())
  }

  def createS3ClientResource(identity: S3Authentication, endpoint: URI): S3Client = {
    S3Client
      .builder()
      .endpointOverride(endpoint)
      .region(Region.of("custom"))
      .credentialsProvider(() => AwsBasicCredentials.create(identity.identity, identity.credential))
      .serviceConfiguration(S3Configuration
        .builder
        .pathStyleAccessEnabled(true)
        .build)
      .build()
  }

  def readKeyToOrder(s3Client: S3Client, bucketName: String, key: String) = {
    val is : InputStream = s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build())
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
