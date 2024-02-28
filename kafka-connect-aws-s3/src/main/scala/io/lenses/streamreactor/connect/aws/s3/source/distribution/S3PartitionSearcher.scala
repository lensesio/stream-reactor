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
package io.lenses.streamreactor.connect.aws.s3.source.distribution

import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3DirectoryLister
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.cloud.common.source.distribution.PartitionSearcherResponse
import io.lenses.streamreactor.connect.cloud.common.source.state.PartitionSearcher
import io.lenses.streamreactor.connect.cloud.common.storage.DirectoryFindCompletionConfig
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response

/**
  * Class implementing a partition searcher for S3 cloud storage.
  * This class searches for new partitions within specified roots in S3.
  *
  * @param roots           The list of root locations in which to search for partitions.
  * @param settings        The configuration options for partition searching.
  * @param connectorTaskId The identifier for the connector task.
  * @param listS3ObjF      A function to list objects in S3 buckets.
  */
class S3PartitionSearcher(
  roots:           Seq[CloudLocation],
  settings:        PartitionSearcherOptions,
  connectorTaskId: ConnectorTaskId,
  listS3ObjF:      ListObjectsV2Request => Iterator[ListObjectsV2Response],
) extends PartitionSearcher
    with LazyLogging {

  /**
    * Finds new partitions based on the provided last found partition responses.
    *
    * @param lastFound The previously found partition responses.
    * @return          A sequence of new partition responses.
    */
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
