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
                          ) {

  BucketNameUtils.validateBucketName(bucket)

  prefix
    .filter(_.contains("/"))
    .foreach(_ => throw new IllegalArgumentException("Nested prefix not currently supported"))
  
  def toPath = BucketAndPath(bucket, prefix.getOrElse(""))
}

case class BucketAndPath(
                          bucket: String,
                          path: String
                        ) {

  BucketNameUtils.validateBucketName(bucket)

}
