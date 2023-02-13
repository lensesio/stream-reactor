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
package io.lenses.streamreactor.connect.aws.s3.sink.seek

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.model.TopicPartition
import io.lenses.streamreactor.connect.aws.s3.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.aws.s3.sink.FatalS3SinkError
import io.lenses.streamreactor.connect.aws.s3.sink.NonFatalS3SinkError
import io.lenses.streamreactor.connect.aws.s3.sink.S3FileNamingStrategy
import io.lenses.streamreactor.connect.aws.s3.sink.SinkError
import io.lenses.streamreactor.connect.aws.s3.storage.FileDeleteError
import io.lenses.streamreactor.connect.aws.s3.storage.FileLoadError
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface

class IndexManager(
  maxIndexes:     Int,
  fallbackSeeker: Option[LegacyOffsetSeeker],
)(
  implicit
  connectorTaskId:  ConnectorTaskId,
  storageInterface: StorageInterface,
) extends OffsetSeeker
    with LazyLogging {

  /**
    * deletes all index files except for the one corresponding to topicPartitionOffset
    *
    * @param mostRecentIndexFile the latest offset successfully written
    * @param topicPartition      the topicPartition
    */
  def clean(mostRecentIndexFile: RemoteS3PathLocation, topicPartition: TopicPartition): Either[SinkError, Int] = {
    val remoteS3PathLocation =
      mostRecentIndexFile.root().withPath(IndexFilenames.indexForTopicPartition(topicPartition.topic.value,
                                                                                topicPartition.partition,
      ))
    storageInterface.list(remoteS3PathLocation)
      .leftMap { e =>
        val logLine = s"Couldn't retrieve listing for (${mostRecentIndexFile.path}})"
        logger.error("[{}] {}", connectorTaskId.show, logLine, e.exception)
        new NonFatalS3SinkError(logLine, e.exception)
      }
      .flatMap {
        indexes: List[String] =>
          val filtered = indexes.filterNot(_ == mostRecentIndexFile.path)
          if (indexes.size > maxIndexes) {
            logAndReturnMaxExceededError(topicPartition, indexes)
          } else if (filtered.size == indexes.size) {
            val logLine = s"Latest file not found in index (${mostRecentIndexFile.path})"
            logger.error("[{}] {}", connectorTaskId.show, logLine)
            NonFatalS3SinkError(logLine).asLeft
          } else {
            storageInterface
              .deleteFiles(mostRecentIndexFile.bucket, filtered)
              .map { _ =>
                logger.debug(
                  "[{}] Retaining index file: {}, Deleting files: ({})",
                  connectorTaskId.show,
                  mostRecentIndexFile.path,
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

  private def logAndReturnMaxExceededError(topicPartition: TopicPartition, indexes: List[String]) = {
    val logLine = s"Too many index files have accumulated (${indexes.size} out of max $maxIndexes)"
    logger.error(s"[{}] {}", connectorTaskId.show, logLine)
    FatalS3SinkError(logLine, topicPartition).asLeft
  }

  def write(
    topicPartitionOffset: TopicPartitionOffset,
    s3PathLocation:       RemoteS3PathLocation,
  ): Either[SinkError, RemoteS3PathLocation] = {

    val s3Path = IndexFilenames.indexFilename(
      topicPartitionOffset.topic.value,
      topicPartitionOffset.partition,
      topicPartitionOffset.offset.value,
    )

    logger.debug("[{}] Writing index {} pointing to file {}", connectorTaskId.show, s3Path, s3PathLocation.path)

    val idxPath = s3PathLocation.root().withPath(s3Path)
    storageInterface
      .writeStringToFile(idxPath, s3PathLocation.path)
      .map(_ => idxPath)
      .leftMap {
        ex =>
          logger.error("[{}] Exception writing index {} pointing to file {}",
                       connectorTaskId.show,
                       s3Path,
                       s3PathLocation.path,
                       ex,
          )
          NonFatalS3SinkError(ex.message())
      }
  }

  def fallbackOffsetSeek(
    seeker:             LegacyOffsetSeeker,
    topicPartition:     TopicPartition,
    fileNamingStrategy: S3FileNamingStrategy,
    bucketAndPrefix:    RemoteS3RootLocation,
  ): Either[SinkError, Option[TopicPartitionOffset]] =
    seeker.seek(topicPartition, fileNamingStrategy, bucketAndPrefix) match {
      case Left(err) =>
        logger.error(
          s"[{}] Error retrieving fallback offset seek topicPartition {} for {}",
          connectorTaskId.show,
          topicPartition,
          bucketAndPrefix.bucket,
        )
        err.asLeft
      case Right(Some((tpo, path))) =>
        write(tpo, path).map(_ => Some(tpo))
      case Right(None) =>
        None.asRight
    }

  /**
    * Seeks the filesystem to find the latyest offsets for a topic/partition.
    *
    * @param topicPartition     the TopicPartition for which to retrieve the offsets
    * @param fileNamingStrategy the S3FileNamingStrategy to use in the case that a fallback offset seeker is required.
    * @param bucketAndPrefix    the configured RemoteS3RootLocation
    * @return either a SinkError or an option to a TopicPartitionOffset with the seek result.
    */
  def seek(
    topicPartition:     TopicPartition,
    fileNamingStrategy: S3FileNamingStrategy,
    bucketAndPrefix:    RemoteS3RootLocation,
  ): Either[SinkError, Option[TopicPartitionOffset]] =
    storageInterface.list(
      bucketAndPrefix.withPath(
        IndexFilenames.indexForTopicPartition(topicPartition.topic.value, topicPartition.partition),
      ),
    )
      .leftMap { e =>
        logger.error("Error retrieving listing", e.exception)
        new NonFatalS3SinkError("Couldn't retrieve listing", e.exception)
      }
      .flatMap {
        indexes =>
          if (indexes.isEmpty && fallbackSeeker.nonEmpty) {
            fallbackOffsetSeek(fallbackSeeker.get, topicPartition, fileNamingStrategy, bucketAndPrefix)
          } else {
            val seekResult = seekAndClean(topicPartition, bucketAndPrefix, indexes)
            if (indexes.size > maxIndexes) {
              logAndReturnMaxExceededError(topicPartition, indexes)
            } else {
              seekResult
            }
          }
      }

  private def seekAndClean(
    topicPartition:  TopicPartition,
    bucketAndPrefix: RemoteS3RootLocation,
    indexes:         List[String],
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
        validIndex     <- scanIndexes(bucketAndPrefix, indexes)
        indexesToDelete = indexes.filterNot(validIndex.contains)
        _              <- storageInterface.deleteFiles(bucketAndPrefix.bucket, indexesToDelete)
        offset         <- indexToOffset(validIndex)
      } yield {
        logger.info("[{}] Seeked offset {} for TP {}", connectorTaskId.show, offset, topicPartition)
        offset
      }
    }.leftMap {
      case err: FileLoadError =>
        val logLine = s"File load error while seeking: ${err.message()}"
        logger.error(s"[{}] {}", connectorTaskId.show, logLine, err.exception)
        new NonFatalS3SinkError(logLine, err.exception)
      case err: FileDeleteError =>
        val logLine = s"File delete error while seeking: ${err.message()}"
        logger.error(s"[{}] {}", connectorTaskId.show, logLine, err.exception)
        new NonFatalS3SinkError(logLine, err.exception)
      case err: Throwable =>
        val logLine = s"Error while seeking: ${err.getMessage}"
        logger.error(s"[{}] {}", connectorTaskId.show, logLine, err)
        new NonFatalS3SinkError(logLine, err)
    }
  }

  /**
    * Given a bucket and a list of files, attempts to load them to establish the most recent valid index
    *
    * @param bucketAndPrefix Remote S3 Root Location of the sink files
    * @param indexFiles      List of index files
    * @return either a FileLoadError or an optional string containing the valid index file of the offset
    */
  def scanIndexes(
    bucketAndPrefix: RemoteS3RootLocation,
    indexFiles:      List[String],
  ): Either[FileLoadError, Option[String]] =
    indexFiles
      .foldRight(
        Option.empty[String].asRight[FileLoadError],
      ) {
        (idxFileName: String, result: Either[FileLoadError, Option[String]]) =>
          result match {
            case Right(None) =>
              for {
                targetFileName     <- storageInterface.getBlobAsString(bucketAndPrefix.withPath(idxFileName))
                targetBucketAndPath = bucketAndPrefix.withPath(targetFileName)
                pathExists         <- storageInterface.pathExists(targetBucketAndPath)
              } yield if (pathExists) Some(idxFileName) else Option.empty[String]
            case Left(_)  => result
            case Right(_) => result
          }
      }
}
