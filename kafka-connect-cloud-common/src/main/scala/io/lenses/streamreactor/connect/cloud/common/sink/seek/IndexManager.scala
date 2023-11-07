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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.naming.IndexFilenames
import io.lenses.streamreactor.connect.cloud.common.storage.FileDeleteError
import io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface

class IndexManager[SM <: FileMetadata](
  maxIndexes: Int,
)(
  implicit
  connectorTaskId:  ConnectorTaskId,
  storageInterface: StorageInterface[SM],
) extends LazyLogging {

  /**
    * deletes all index files except for the one corresponding to topicPartitionOffset
    *
    * @param mostRecentIndexFile the latest offset successfully written
    * @param topicPartition      the topicPartition
    */
  def clean(bucket: String, mostRecentIndexFile: String, topicPartition: TopicPartition): Either[SinkError, Int] = {
    val indexFileLocation =
      IndexFilenames.indexForTopicPartition(
        topicPartition.topic.value,
        topicPartition.partition,
      )
    storageInterface.listKeysRecursive(
      bucket,
      indexFileLocation.some,
    )
      .leftMap { e =>
        val logLine = s"Couldn't retrieve listing for (${mostRecentIndexFile}})"
        logger.error("[{}] {}", connectorTaskId.show, logLine, e.exception)
        new NonFatalCloudSinkError(logLine, e.exception)
      }
      .flatMap {
        case None => 0.asRight
        case Some(listResponse) =>
          val indexes  = listResponse.files
          val filtered = indexes.filterNot(_ == mostRecentIndexFile)
          if (indexes.size > maxIndexes) {
            logAndReturnMaxExceededError(topicPartition, indexes)
          } else if (filtered.size == indexes.size) {
            val logLine = s"Latest file not found in index ($mostRecentIndexFile)"
            logger.error("[{}] {}", connectorTaskId.show, logLine)
            NonFatalCloudSinkError(logLine).asLeft
          } else {
            storageInterface
              .deleteFiles(bucket, filtered)
              .map { _ =>
                logger.debug(
                  "[{}] Retaining index file: {}, Deleting files: ({})",
                  connectorTaskId.show,
                  mostRecentIndexFile,
                  filtered.mkString(", "),
                )
                filtered.size
              }
              .leftMap {
                err =>
                  logger.error("[{}] Error on cleanup: {}", connectorTaskId.show, err.message(), err.exception)
              }.getOrElse(0).asRight
          }
      }
  }

  private def logAndReturnMaxExceededError(topicPartition: TopicPartition, indexes: Seq[String]) = {
    val logLine = s"Too many index files have accumulated (${indexes.size} out of max $maxIndexes)"
    logger.error(s"[{}] {}", connectorTaskId.show, logLine)
    FatalCloudSinkError(logLine, topicPartition).asLeft
  }

  def write(
    bucket:               String,
    filePath:             String,
    topicPartitionOffset: TopicPartitionOffset,
  ): Either[SinkError, String] = {

    val indexPath = IndexFilenames.indexFilename(
      topicPartitionOffset.topic.value,
      topicPartitionOffset.partition,
      topicPartitionOffset.offset.value,
    )

    logger.debug("[{}] Writing index {} pointing to file {}", connectorTaskId.show, indexPath, filePath)

    storageInterface
      .writeStringToFile(bucket, indexPath, UploadableString(filePath))
      .map(_ => indexPath)
      .leftMap {
        ex =>
          logger.error("[{}] Exception writing index {} pointing to file {}",
                       connectorTaskId.show,
                       indexPath,
                       filePath,
                       ex,
          )
          NonFatalCloudSinkError(ex.message())
      }

  }

  /**
    * Seeks the filesystem to find the latyest offsets for a topic/partition.
    *
    * @param topicPartition     the TopicPartition for which to retrieve the offsets
    * @param bucket    the configured bucket
    * @return either a SinkError or an option to a TopicPartitionOffset with the seek result.
    */
  def seek(
    topicPartition: TopicPartition,
    bucket:         String,
  ): Either[SinkError, Option[TopicPartitionOffset]] = {
    val indexLocation = IndexFilenames.indexForTopicPartition(topicPartition.topic.value, topicPartition.partition)
    storageInterface.listKeysRecursive(
      bucket,
      indexLocation.some,
    )
      .leftMap { e =>
        logger.error("Error retrieving listing", e.exception)
        new NonFatalCloudSinkError("Couldn't retrieve listing", e.exception)
      }
      .flatMap {
        case None => Option.empty[TopicPartitionOffset].asRight[SinkError]
        case Some(response) =>
          val bucket     = response.bucket
          val files      = response.files
          val seekResult = seekAndClean(topicPartition, bucket, files)
          if (files.size > maxIndexes) {
            logAndReturnMaxExceededError(topicPartition, files)
          } else {
            seekResult
          }
      }
  }

  private def seekAndClean(
    topicPartition: TopicPartition,
    bucket:         String,
    indexes:        Seq[String],
  ) = {

    /**
      * Parses the filename of the index file, converting it to a TopicPartitionOffset
      *
      * @param maybeIndex option of the index filename
      * @return either an error, or a TopicPartitionOffset
      */
    def indexToOffset(maybeIndex: Option[String]): Either[Throwable, Option[TopicPartitionOffset]] =
      maybeIndex match {
        case Some(index) =>
          for {
            offset <- IndexFilenames.offsetFromIndex(index)
          } yield Some(topicPartition.withOffset(offset))
        case None => Option.empty[TopicPartitionOffset].asRight
      }

    {
      for {
        validIndex     <- scanIndexes(bucket, indexes)
        indexesToDelete = indexes.filterNot(validIndex.contains)
        _              <- storageInterface.deleteFiles(bucket, indexesToDelete)
        offset         <- indexToOffset(validIndex)
      } yield {
        logger.info("[{}] Seeked offset {} for TP {}", connectorTaskId.show, offset, topicPartition)
        offset
      }
    }.leftMap {
      case err: FileLoadError =>
        val logLine = s"File load error while seeking: ${err.message()}"
        logger.error(s"[{}] {}", connectorTaskId.show, logLine, err.exception)
        new NonFatalCloudSinkError(logLine, err.exception)
      case err: FileDeleteError =>
        val logLine = s"File delete error while seeking: ${err.message()}"
        logger.error(s"[{}] {}", connectorTaskId.show, logLine, err.exception)
        new NonFatalCloudSinkError(logLine, err.exception)
      case err: Throwable =>
        val logLine = s"Error while seeking: ${err.getMessage}"
        logger.error(s"[{}] {}", connectorTaskId.show, logLine, err)
        new NonFatalCloudSinkError(logLine, err)
    }
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
