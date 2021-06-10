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

package io.lenses.streamreactor.connect.aws.s3.model

import com.amazonaws.services.s3.internal.BucketNameUtils
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.sink.S3WriterManager.logger

import java.io.File
import java.util.UUID


case object BucketAndPrefix {
  def apply(bucketAndPath: String): BucketAndPrefix = {
    bucketAndPath.split(":") match {
      case Array(bucket) => BucketAndPrefix(bucket, None)
      case Array(bucket, path) => BucketAndPrefix(bucket, Some(path))
      case _ => throw new IllegalArgumentException("Invalid number of arguments provided to create BucketAndPrefix")
    }
  }
}

case class BucketAndPrefix(
                            bucket: String,
                            prefix: Option[String]
                          )  extends Location {

  BucketNameUtils.validateBucketName(bucket)

  prefix
    .filter(_.contains("/"))
    .foreach(_ => throw new IllegalArgumentException("Nested prefix not currently supported"))
}

trait Location {

}

object LocalLocation extends LazyLogging {
  def apply(parentDir : LocalLocation, bucketAndPath: BucketAndPath): LocalLocation = {
    val uuid = UUID.randomUUID().toString

    val dir = new File(s"${parentDir.path}/${bucketAndPath.bucket}/${bucketAndPath.path}")
    logger.info("Creating dir {}", dir)
    dir.mkdirs()

    val file = new File(dir, s"/$uuid")
    logger.info("Creating file {}", file)
    file.createNewFile()
    LocalLocation(file.getAbsolutePath)
  }
}
case class LocalLocation (
                           path: String,
                         ) extends Location {



}

case class BucketAndPath (
                          bucket: String,
                          path: String
                        ) extends Location {

  BucketNameUtils.validateBucketName(bucket)

}
