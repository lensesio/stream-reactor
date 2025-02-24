/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.seek.deprecated

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerErrors.corruptStorageState
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerErrors.fileDeleteError
import io.lenses.streamreactor.connect.cloud.common.storage._

class IndexManagerV1(
  indexFilenames:    IndexFilenames,
  bucketAndPrefixFn: TopicPartition => Either[SinkError, CloudLocation],
)(
  implicit
  connectorTaskId:  ConnectorTaskId,
  storageInterface: StorageInterface[_],
) extends LazyLogging {

  /**
    * Seeks the filesystem to find the latest offset for a specific `TopicPartition`.
    *
    * This method is used during the initialization of a Kafka Connect SinkTask to find the latest offset for a specific `TopicPartition`.
    * The result is stored in the `seekedOffsets` map for later use.
    *
    * @param topicPartition The `TopicPartition` for which to retrieve the offset.
    * @return Either a `SinkError` if an error occurred during the operation, or an `Option[TopicPartitionOffset]` containing the seeked offset for the `TopicPartition`.
    */
  def seekOffsetsForTopicPartition(
    topicPartition: TopicPartition,
  ): Either[SinkError, Option[TopicPartitionOffset]] = {
    logger.debug(s"[{}] seekOffsetsForTopicPartition {}", connectorTaskId.show, topicPartition)
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      offset <- {
        val indexLocation = indexFilenames.indexForTopicPartition(topicPartition.topic.value, topicPartition.partition)
        storageInterface.listKeysRecursive(
          bucketAndPrefix.bucket,
          indexLocation.some,
        )
          .leftMap { e =>
            logger.error("Error retrieving listing", e.exception)
            new NonFatalCloudSinkError("Couldn't retrieve listing", Option(e.exception))
          }
          .flatMap {
            case None => Option.empty[TopicPartitionOffset].asRight[SinkError]
            case Some(response) =>
              val bucket = response.bucket
              val files  = response.files
              seekAndClean(topicPartition, bucket, files)
          }
      }
    } yield offset
  }

  private def seekAndClean(
    topicPartition: TopicPartition,
    bucket:         String,
    indexes:        Seq[String],
  ): Either[NonFatalCloudSinkError, Option[TopicPartitionOffset]] = {
    for {
      validIndex     <- scanIndexes(bucket, indexes)
      indexesToDelete = indexes.filterNot(validIndex.contains)
      _              <- storageInterface.deleteFiles(bucket, indexesToDelete)
      offset         <- indexFilenames.indexToOffset(topicPartition, validIndex).leftMap(FileNameParseError(_, s"$validIndex"))
    } yield {
      logger.info("[{}] Seeked offset {} for TP {}", connectorTaskId.show, offset, topicPartition)
      offset
    }
  }.leftMap(e => handleSeekAndCleanErrors(e))

  def handleSeekAndCleanErrors(uploadError: UploadError): NonFatalCloudSinkError =
    uploadError match {
      case err: FileLoadError =>
        val logLine = s"File load error while seeking: ${err.message()}"
        logger.error(s"[{}] {}", connectorTaskId.show, logLine, err.exception)
        NonFatalCloudSinkError(corruptStorageState(storageInterface.system()))
      case err: FileDeleteError =>
        val logLine = s"File delete error while seeking: ${err.message()}"
        logger.error(s"[{}] {}", connectorTaskId.show, logLine, err.exception)
        NonFatalCloudSinkError(fileDeleteError(storageInterface.system()))
      case err: FileNameParseError =>
        val logLine = s"Error while seeking: ${err.message()}"
        logger.error(s"[{}] {}", connectorTaskId.show, logLine, err)
        new NonFatalCloudSinkError(logLine, err.exception.some)
    }

  /**
    * Given a bucket and a list of files, attempts to load them to establish the most recent valid index
    *
    * @param bucket    the configured bucket
    * @param indexFiles      List of index files
    * @return either a FileLoadError or an optional string containing the valid index file of the offset
    */
  def scanIndexes(
    bucket:     String,
    indexFiles: Seq[String],
  ): Either[FileLoadError, Option[String]] =
    indexFiles
      .foldRight(
        Option.empty[String].asRight[FileLoadError],
      ) {
        (idxFileName: String, result: Either[FileLoadError, Option[String]]) =>
          result match {
            case Right(None) =>
              for {
                targetFileName <- storageInterface.getBlobAsString(bucket, idxFileName)
                pathExists     <- storageInterface.pathExists(bucket, targetFileName)
              } yield if (pathExists) Some(idxFileName) else Option.empty[String]
            case _ => result
          }
      }

}
