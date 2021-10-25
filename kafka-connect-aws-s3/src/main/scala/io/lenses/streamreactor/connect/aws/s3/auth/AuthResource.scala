package io.lenses.streamreactor.connect.aws.s3.auth
import io.lenses.streamreactor.connect.aws.s3.config.S3Config
import org.jclouds.blobstore.BlobStoreContext
import software.amazon.awssdk.services.s3.S3Client

import scala.util.Try

trait AuthResource[R] {

  def create(awsConfig: S3Config): Either[String, R]

}

class AuthResources(awsConfig: S3Config) {

  def aws: Either[String,S3Client] = AwsAuthResourceCreator.create(awsConfig)

  def jClouds: Either[String, BlobStoreContext] = JcloudsAuthResourceCreator.create(awsConfig)

}

object JcloudsAuthResourceCreator extends AuthResource[BlobStoreContext] {

  override def create(awsConfig: S3Config): Either[String, BlobStoreContext] = {
    Try(new JCloudsS3ContextCreator(JCloudsS3ContextCreator.DefaultCredentialsFn)
      .fromConfig(awsConfig)).toEither.left.map(_.getMessage)
  }

}

object AwsAuthResourceCreator extends AuthResource[S3Client] {

  override def create(awsConfig: S3Config): Either[String, S3Client] = {
    new AwsS3ClientCreator(awsConfig).createS3Client()
  }

}
