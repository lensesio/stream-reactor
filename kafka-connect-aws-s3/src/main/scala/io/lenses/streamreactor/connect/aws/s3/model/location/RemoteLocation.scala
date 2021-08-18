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

package io.lenses.streamreactor.connect.aws.s3.model.location

import com.amazonaws.services.s3.internal.BucketNameUtils

import java.util.UUID

case object RemoteS3RootLocation {
  def apply(bucketAndPath: String): RemoteS3RootLocation = {
    bucketAndPath.split(":") match {
      case Array(bucket) => RemoteS3RootLocation(bucket, None)
      case Array(bucket, path) => RemoteS3RootLocation(bucket, Some(path))
      case _ => throw new IllegalArgumentException("Invalid number of arguments provided to create BucketAndPrefix")
    }
  }

}

case class RemoteS3RootLocation(
                               bucket: String,
                               prefix: Option[String]
                             ) extends RootLocation[RemoteS3PathLocation] {

  BucketNameUtils.validateBucketName(bucket)

  prefix
    .filter(_.contains("/"))
    .foreach(_ => throw new IllegalArgumentException("Nested prefix not currently supported"))

  override def withPath(path: String) = RemoteS3PathLocation(bucket, path)

}

case class RemoteS3PathLocation(
                               bucket: String,
                               override val path: String
                             ) extends PathLocation {

  /**
    * Given a LocalRootLocation, returns a new LocalPathLocation with the path including the remote location.
    */
  def toLocalPathLocation(localRoot: LocalRootLocation): LocalPathLocation = {
    val uuid = UUID.randomUUID().toString
    localRoot.withPath(s"${localRoot.basePath}/$bucket/$path/$uuid")
  }

  BucketNameUtils.validateBucketName(bucket)

}
