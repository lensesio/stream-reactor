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

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import software.amazon.awssdk.services.s3.model._

import scala.jdk.CollectionConverters.IteratorHasAsScala

object AwsS3DirectoryLister extends LazyLogging {
  def findDirectories(
    bucketAndPrefix:  S3Location,
    completionConfig: DirectoryFindCompletionConfig,
    exclude:          Set[String],
    wildcardExclude:  Set[String],
    listObjectsF:     ListObjectsV2Request => Iterator[ListObjectsV2Response],
    connectorTaskId:  ConnectorTaskId,
  ): IO[DirectoryFindResults] =
    for {
      iterator <- IO(listObjectsF(createListObjectsRequest(bucketAndPrefix)))
      prefixInfo <- extractPrefixesFromResponse(iterator,
                                                exclude,
                                                wildcardExclude,
                                                connectorTaskId,
                                                completionConfig.levelsToRecurse,
      )
      flattened <- flattenPrefixes(
        bucketAndPrefix,
        prefixInfo.partitions,
        completionConfig,
        exclude,
        wildcardExclude,
        listObjectsF,
        connectorTaskId,
      )
    } yield DirectoryFindResults(flattened)

  private def flattenPrefixes(
    bucketAndPrefix:  S3Location,
    prefixes:         Set[String],
    completionConfig: DirectoryFindCompletionConfig,
    exclude:          Set[String],
    wildcardExclude:  Set[String],
    listObjectsF:     ListObjectsV2Request => Iterator[ListObjectsV2Response],
    connectorTaskId:  ConnectorTaskId,
  ): IO[Set[String]] =
    if (completionConfig.levelsToRecurse <= 0) IO.delay(prefixes)
    else {
      prefixes.map(bucketAndPrefix.fromRoot).toList
        .traverse(
          findDirectories(
            _,
            completionConfig.copy(levelsToRecurse = completionConfig.levelsToRecurse - 1),
            exclude,
            wildcardExclude,
            listObjectsF,
            connectorTaskId,
          ).map(_.partitions),
        )
        .map { result =>
          result.foldLeft(Set.empty[String])(_ ++ _)
        }
    }

  private def createListObjectsRequest(
    bucketAndPrefix: S3Location,
  ): ListObjectsV2Request = {

    val builder = ListObjectsV2Request
      .builder()
      .maxKeys(1000)
      .bucket(bucketAndPrefix.bucket)
      .delimiter("/")
    bucketAndPrefix.prefix.foreach(builder.prefix)
    builder.build()
  }

  private def extractPrefixesFromResponse(
    iterator:        Iterator[ListObjectsV2Response],
    exclude:         Set[String],
    wildcardExclude: Set[String],
    connectorTaskId: ConnectorTaskId,
    levelsToRecurse: Int,
  ): IO[DirectoryFindResults] =
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
                    connectorTaskId.ownsDir(prefix) && !exclude.contains(prefix) && !wildcardExclude.exists(we =>
                      prefix.contains(we),
                    )
                  ) acc + prefix
                  else acc
                }
              }
          acc ++ commonPrefixesFiltered
      }
      DirectoryFindResults(paths)
    }
}
