/*
 * Copyright 2017-2026 Lenses.io Ltd
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
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.naming.IndexFilenames
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerErrors.corruptStorageState
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerErrors.fileDeleteError
import io.lenses.streamreactor.connect.cloud.common.storage._

import scala.collection.mutable

class IndexManager[SM <: FileMetadata](
  maxIndexes:        Int,
  indexFilenames:    IndexFilenames,
  bucketAndPrefixFn: TopicPartition => Either[SinkError, CloudLocation],
)(
  implicit
  connectorTaskId:  ConnectorTaskId,
  storageInterface: StorageInterface[SM],
) extends LazyLogging {

  // A mutable map that stores the latest offset for each TopicPartition that was seeked during the initialization of the Kafka Connect SinkTask.
  // The key is the TopicPartition and the value is the corresponding Offset.
  private val seekedOffsets = mutable.Map.empty[TopicPartition, Offset]

  /**
    * deletes all index files except for the one corresponding to topicPartitionOffset
    *
    * @param mostRecentIndexFile the latest offset successfully written
    * @param topicPartition      the topicPartition
    */
  def clean(bucket: String, mostRecentIndexFile: String, topicPartition: TopicPartition): Either[SinkError, Int] = {
    val indexFileLocation =
      indexFilenames.indexForTopicPartition(
        topicPartition.topic.value,
        topicPartition.partition,
      )
    storageInterface.listKeysRecursive(
      bucket,
      indexFileLocation.some,
    )
      .leftMap { e =>
        val logLine = s"Couldn't retrieve listing for ($mostRecentIndexFile})"
        logger.error("[{}] {}", connectorTaskId.show, logLine, e.exception)
        new NonFatalCloudSinkError(logLine, e.exception.some)
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

    val indexPath = indexFilenames.indexFilename(
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
    * Seeks the filesystem to find the latest offsets for a topic/partition.
    *
    * @param topicPartition     the TopicPartition for which to retrieve the offsets
    * @param bucket    the configured bucket
    * @return either a SinkError or an option to a TopicPartitionOffset with the seek result.
    */
  def initialSeek(
    topicPartition: TopicPartition,
    bucket:         String,
  ): Either[SinkError, Option[TopicPartitionOffset]] = {
    val indexLocation = indexFilenames.indexForTopicPartition(topicPartition.topic.value, topicPartition.partition)
    storageInterface.listKeysRecursive(
      bucket,
      indexLocation.some,
    )
      .leftMap { e =>
        logger.error("Error retrieving listing", e.exception)
        new NonFatalCloudSinkError("Couldn't retrieve listing", Option(e.exception))
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

  def seekAndClean(
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

  /**
    * Opens the `IndexManager` for a set of `TopicPartition`s.
    *
    * This method is called at the start of a Kafka Connect SinkTask when it is first initialized.
    * It seeks the filesystem to find the latest offsets for each `TopicPartition` in the provided set.
    * The results are stored in the `seekedOffsets` map for later use.
    *
    * @param partitions A set of `TopicPartition`s for which to retrieve the offsets.
    * @return Either a `SinkError` if an error occurred during the operation, or a `Map[TopicPartition, Offset]` containing the seeked offsets for each `TopicPartition`.
    */
  def open(partitions: Set[TopicPartition]): Either[SinkError, Map[TopicPartition, Offset]] = {
    logger.debug(s"[{}] Received call to WriterManager.open", connectorTaskId.show)

    partitions
      .map(seekOffsetsForTopicPartition)
      .partitionMap(identity) match {
      case (throwables, _) if throwables.nonEmpty => BatchCloudSinkError(throwables).asLeft
      case (_, offsets) =>
        val seeked = offsets.flatten.map(
          _.toTopicPartitionOffsetTuple,
        ).toMap
        seekedOffsets ++= seeked
        seeked.asRight
    }
  }

  /**
    * Seeks the filesystem to find the latest offset for a specific `TopicPartition`.
    *
    * This method is used during the initialization of a Kafka Connect SinkTask to find the latest offset for a specific `TopicPartition`.
    * The result is stored in the `seekedOffsets` map for later use.
    *
    * @param topicPartition The `TopicPartition` for which to retrieve the offset.
    * @return Either a `SinkError` if an error occurred during the operation, or an `Option[TopicPartitionOffset]` containing the seeked offset for the `TopicPartition`.
    */
  private def seekOffsetsForTopicPartition(
    topicPartition: TopicPartition,
  ): Either[SinkError, Option[TopicPartitionOffset]] = {
    logger.debug(s"[{}] seekOffsetsForTopicPartition {}", connectorTaskId.show, topicPartition)
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      offset          <- initialSeek(topicPartition, bucketAndPrefix.bucket)
    } yield offset
  }

  /**
    * Retrieves the latest offset for a specific `TopicPartition` that was seeked during the initialization of the Kafka Connect SinkTask.
    *
    * This method is used to get the latest offset for a specific `TopicPartition` from the `seekedOffsets` map.
    *
    * @param topicPartition The `TopicPartition` for which to retrieve the offset.
    * @return An `Option[Offset]` containing the seeked offset for the `TopicPartition` if it exists, or `None` if it does not.
    */
  def getSeekedOffsetForTopicPartition(topicPartition: TopicPartition): Option[Offset] =
    seekedOffsets.get(topicPartition)

}
