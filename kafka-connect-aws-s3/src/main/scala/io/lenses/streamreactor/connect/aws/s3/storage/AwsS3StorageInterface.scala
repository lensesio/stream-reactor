/*
 * Copyright 2021 Lenses.io
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
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.location.{RemoteS3PathLocation, RemoteS3RootLocation}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request

import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.Try

class AwsS3StorageInterface(s3Client: S3Client) extends SourceStorageInterface with LazyLogging {

  override def list(bucketAndPrefix: RemoteS3RootLocation, lastFile: Option[RemoteS3PathLocation], numResults: Int): Either[Throwable, List[String]] = {

    Try {

      val builder = ListObjectsV2Request
        .builder()
        .maxKeys(numResults)
        .bucket(bucketAndPrefix.bucket)

      bucketAndPrefix.prefix.foreach(builder.prefix)
      lastFile.foreach(lf => builder.startAfter(lf.path))

      val listObjectsV2Response = s3Client.listObjectsV2(builder.build())
      listObjectsV2Response.contents().asScala.map(_.key()).toList

    }.toEither
  }

  override def pathExists(bucketAndPrefix: RemoteS3RootLocation): Either[Throwable, Boolean] = {

    Try {

      val builder = ListObjectsV2Request
        .builder()
        .maxKeys(1)
        .bucket(bucketAndPrefix.bucket)

      bucketAndPrefix.prefix.foreach(builder.prefix)

      s3Client.listObjectsV2(builder.build()).hasContents

    }.toEither
  }

}
