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
package io.lenses.streamreactor.connect.aws.s3.source

import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.ContextConstants._

import java.time.Instant
import scala.jdk.CollectionConverters.MapHasAsJava

object SourceWatermark {

  /**
    * Builds the source partition information from the S3Location
    * @param root The S3Location
    * @return A map of partition information
    */
  def partition(root: S3Location): java.util.Map[String, String] =
    Map(
      ContainerKey -> root.bucket,
      PrefixKey    -> root.prefixOrDefault(),
    ).asJava

  /**
    * Builds the source offset information from the S3Location
    * @param bucketAndPath The S3Location
    * @param offset The offset
    * @param lastModified The last modified time of the file processed
    * @return A map of offset information
    */
  def offset(bucketAndPath: S3Location, offset: Long, lastModified: Instant): java.util.Map[String, String] =
    Map(
      PathKey      -> bucketAndPath.pathOrUnknown,
      LineKey      -> offset.toString,
      TimeStampKey -> lastModified.toEpochMilli.toString,
    ).asJava

}
