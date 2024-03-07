/*
 * Copyright 2017-2024 Lenses.io Ltd
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

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.storage.DirectoryLister
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import scala.jdk.CollectionConverters.IteratorHasAsScala

class AwsS3DirectoryLister(connectorTaskId: ConnectorTaskId, s3Client: S3Client)
    extends LazyLogging
    with DirectoryLister {

  private val listObjectsF: ListObjectsV2Request => IO[Iterator[ListObjectsV2Response]] = e =>
    IO(s3Client.listObjectsV2Paginator(e).iterator().asScala)

  /**
    * @param wildcardExcludes allows ignoring paths containing certain strings.  Mainly it is used to prevent us from reading anything inside the .indexes key prefix, as these should be ignored by the source.
    */
  override def findDirectories(
    bucketAndPrefix:  CloudLocation,
    filesLimit:       Int,
    recurseLevels:    Int,
    exclude:          Set[String],
    wildcardExcludes: Set[String],
  ): IO[Set[String]] =
    for {
      iterator   <- listObjects(bucketAndPrefix)
      prefixInfo <- extractPrefixesFromResponse(iterator, exclude, wildcardExcludes, recurseLevels)
      flattened  <- flattenPrefixes(bucketAndPrefix, filesLimit, prefixInfo, recurseLevels, exclude, wildcardExcludes)
    } yield flattened

  private def listObjects(bucketAndPrefix: CloudLocation): IO[Iterator[ListObjectsV2Response]] = {

    def createListObjectsRequest(
      bucketAndPrefix: CloudLocation,
    ): ListObjectsV2Request = {

      val builder = ListObjectsV2Request
        .builder()
        .maxKeys(1000)
        .bucket(bucketAndPrefix.bucket)
        .delimiter("/")
      bucketAndPrefix.prefix.foreach(builder.prefix)
      builder.build()
    }

    listObjectsF(createListObjectsRequest(bucketAndPrefix))
  }

  /**
    * @param wildcardExcludes allows ignoring paths containing certain strings.  Mainly it is used to prevent us from reading anything inside the .indexes key prefix, as these should be ignored by the source.
    */
  private def flattenPrefixes(
    bucketAndPrefix:  CloudLocation,
    filesLimit:       Int,
    prefixes:         Set[String],
    recurseLevels:    Int,
    exclude:          Set[String],
    wildcardExcludes: Set[String],
  ): IO[Set[String]] =
    if (recurseLevels <= 0) IO.delay(prefixes)
    else {
      prefixes.map(bucketAndPrefix.fromRoot).toList
        .traverse((bucketAndPrefix: CloudLocation) =>
          findDirectories(bucketAndPrefix, filesLimit, recurseLevels - 1, exclude, wildcardExcludes),
        )
        .map { result =>
          result.foldLeft(Set.empty[String])(_ ++ _)
        }
    }

  private def extractPrefixesFromResponse(
    iterator:         Iterator[ListObjectsV2Response],
    exclude:          Set[String],
    wildcardExcludes: Set[String],
    levelsToRecurse:  Int,
  ): IO[Set[String]] =
    IO {
      val paths = iterator.foldLeft(Set.empty[String]) {
        case (acc, listResp) =>
          val commonPrefixesFiltered =
            Option(listResp.commonPrefixes()).map(_.iterator().asScala).getOrElse(Iterator.empty)
              .foldLeft(Set.empty[String]) { (acc, item) =>
                val prefix = item.prefix()
                if (levelsToRecurse > 0) {
                  acc + prefix
                } else {
                  if (
                    connectorTaskId.ownsDir(prefix) && !exclude.contains(prefix) && !wildcardExcludes.exists(we =>
                      prefix.contains(we),
                    )
                  ) acc + prefix
                  else acc
                }
              }
          acc ++ commonPrefixesFiltered
      }
      paths
    }
}
