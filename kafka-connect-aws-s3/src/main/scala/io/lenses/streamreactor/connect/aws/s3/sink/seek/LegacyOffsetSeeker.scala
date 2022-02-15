
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

package io.lenses.streamreactor.connect.aws.s3.sink.seek

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.location.{RemoteS3PathLocation, RemoteS3RootLocation}
import io.lenses.streamreactor.connect.aws.s3.model.{Offset, TopicPartition, TopicPartitionOffset}
import io.lenses.streamreactor.connect.aws.s3.sink._
import io.lenses.streamreactor.connect.aws.s3.storage.{FileListError, StorageInterface}

import scala.util.Try

/**
 * The [[LegacyOffsetSeeker]] is responsible for querying the [[StorageInterface]] to
 * retrieve current offset information from a container.
 *
 * @deprecated and will be removed once migration has occured
 * @param fileNamingStrategy we need the policy so we can match on this.
 */
@Deprecated
class LegacyOffsetSeeker(sinkName: String)(implicit storageInterface: StorageInterface) extends LazyLogging {

  def seek(topicPartition: TopicPartition, fileNamingStrategy: S3FileNamingStrategy, bucketAndPrefix: RemoteS3RootLocation): Either[SinkError, Option[(TopicPartitionOffset, RemoteS3PathLocation)]] = {
    logger.debug(s"[{}] seekOffsetsForTopicPartition {}", sinkName, topicPartition)
    for {
      topicPartitionRoot <- Try(fileNamingStrategy.topicPartitionPrefix(bucketAndPrefix, topicPartition))
        .toEither.leftMap(ex => FatalS3SinkError(ex.getMessage, ex, topicPartition))
      seeked <- seek(fileNamingStrategy, topicPartition, topicPartitionRoot)
    } yield {
      logger.info("[{}] Seeked {} {}", sinkName, bucketAndPrefix.toString, seeked)
      seeked
    }
  }

  private def seek(fileNamingStrategy: S3FileNamingStrategy, topicPartition: TopicPartition, bucketAndPath: RemoteS3PathLocation): Either[SinkError, Option[(TopicPartitionOffset, RemoteS3PathLocation)]] = {
    implicit val impFileNamingStrategy: S3FileNamingStrategy = fileNamingStrategy

    storageInterface
      .list(bucketAndPath)
      .leftMap {
        error: FileListError => NonFatalS3SinkError(error.message())
      }.map {
      pathList =>
        if (pathList.nonEmpty) {
          val (path: String, offset: Offset) = pathList
            .collect {
              case file@CommittedFileName(topic, partition, end, format)
                if format == fileNamingStrategy.getFormat && topic == topicPartition.topic && partition == topicPartition.partition =>
                file -> end
            }
            .maxBy(_._2.value)
          Some((topicPartition.withOffset(offset), bucketAndPath.root().withPath(path)))
        } else {
          Option.empty[(TopicPartitionOffset, RemoteS3PathLocation)]
        }
    }

  }

}

