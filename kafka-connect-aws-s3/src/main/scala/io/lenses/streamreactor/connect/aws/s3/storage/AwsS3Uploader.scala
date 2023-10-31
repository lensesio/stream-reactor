/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.storage

import cats.implicits.toBifunctorOps
import cats.implicits.toShow
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import java.nio.ByteBuffer
import scala.util.Try

class AwsS3Uploader(s3Client: S3Client, connectorTaskId: ConnectorTaskId) extends Uploader with LazyLogging {
  override def close(): Unit = s3Client.close()

  override def upload(source: ByteBuffer, bucket: String, path: String): Either[Throwable, Unit] = {
    logger.debug(s"[{}] AWS Uploading to s3 {}:{}", connectorTaskId.show, source, bucket, path)
    Try {
      s3Client.putObject(
        PutObjectRequest.builder()
          .bucket(bucket)
          .key(path)
          .contentLength(source.remaining())
          .build(),
        RequestBody.fromByteBuffer(source),
      )
      logger.debug(s"[{}] Completed upload to s3 {}:{}", connectorTaskId.show, source, bucket, path)
    }
      .toEither.leftMap { ex =>
        logger.error(s"[{}] Failed upload to s3 {}:{}", connectorTaskId.show, source, bucket, path, ex)
        ex
      }
  }

  override def delete(bucket: String, path: String): Either[Throwable, Unit] = {
    logger.debug(s"[{}] AWS Deleting from s3 {}:{}", connectorTaskId.show, bucket, path)
    Try {
      s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(path).build())
      logger.debug(s"[{}] Completed delete from s3 {}:{}", connectorTaskId.show, bucket, path)
    }
      .toEither.leftMap { ex =>
        logger.error(s"[{}] Failed delete from s3 {}:{}", connectorTaskId.show, bucket, path, ex)
        ex
      }
  }
}
