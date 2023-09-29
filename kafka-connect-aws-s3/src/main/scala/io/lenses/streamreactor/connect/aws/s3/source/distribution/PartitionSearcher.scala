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
package io.lenses.streamreactor.connect.aws.s3.source.distribution

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3DirectoryLister
import io.lenses.streamreactor.connect.aws.s3.storage.DirectoryFindCompletionConfig
import io.lenses.streamreactor.connect.cloud.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.model.location.CloudLocation
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response

class PartitionSearcher(
  roots:           Seq[CloudLocation],
  settings:        PartitionSearcherOptions,
  connectorTaskId: ConnectorTaskId,
  listS3ObjF:      ListObjectsV2Request => Iterator[ListObjectsV2Response],
) extends LazyLogging {

  def find(
    lastFound: Seq[PartitionSearcherResponse],
  ): IO[Seq[PartitionSearcherResponse]] =
    if (lastFound.isEmpty) {
      roots.traverse(findNewPartitionsInRoot(_, settings, Set.empty))
    } else {
      lastFound.traverse {
        prevResponse =>
          findNewPartitionsInRoot(
            prevResponse.root,
            settings,
            prevResponse.allPartitions,
          )
      }
    }

  private def findNewPartitionsInRoot(
    root:               CloudLocation,
    settings:           PartitionSearcherOptions,
    originalPartitions: Set[String],
  ): IO[PartitionSearcherResponse] = {
    val config = DirectoryFindCompletionConfig.fromSearchOptions(settings)
    for {
      foundPartitions <- AwsS3DirectoryLister.findDirectories(
        root,
        config,
        originalPartitions,
        settings.wildcardExcludes,
        listS3ObjF,
        connectorTaskId,
      )
      _ <- IO {
        if (foundPartitions.partitions.nonEmpty)
          logger.info("[{}] Found new partitions {} for: {}",
                      connectorTaskId.show,
                      foundPartitions.partitions.mkString(","),
                      root.show,
          )
        else
          logger.info("[{}] No new partitions found for:{}", connectorTaskId.show, root.show)
      }

    } yield PartitionSearcherResponse(
      root,
      originalPartitions ++ foundPartitions.partitions,
      foundPartitions,
      Option.empty,
    )
  }

}
