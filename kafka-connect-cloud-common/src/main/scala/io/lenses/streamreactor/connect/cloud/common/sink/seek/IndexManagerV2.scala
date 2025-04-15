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
package io.lenses.streamreactor.connect.cloud.common.sink.seek
import cats.data.EitherT
import cats.effect.IO
import cats.effect.IO._
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerV2.generateLockFilePath
import io.lenses.streamreactor.connect.cloud.common.sink.seek.deprecated.IndexManagerV1
import io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.FileNotFoundError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError

import scala.collection.mutable

/**
  * A class that implements the `IndexManager` trait to manage indexing operations
  * for a cloud sink. This implementation uses a mutable map to track seeked offsets
  * and eTags for index files, enabling efficient handling of file operations.
  *
  * Pending operations are processed using the `PendingOperationsProcessors` class,
  * which ensures that any task picking up the work can resume and complete the pending
  * operations before processing new offsets. The index files are updated after each
  * operation to reflect the new state, including the updated list of pending operations
  * and the latest committed offset. This mechanism ensures fault tolerance and consistency
  * in the event of task failures or restarts.
  *
  * The original IndexManager, `IndexManagerV1`, is used for migration of stored offset files only and will be removed in
  * a future version.
  *
  * @param bucketAndPrefixFn           A function that maps a `TopicPartition` to an `Either` containing
  *                                    a `SinkError` or a `CloudLocation`.
  * @param oldIndexManager             An instance of `IndexManagerV1` used for seeking offsets.
  * @param pendingOperationsProcessors A processor for handling pending operations.
  * @param storageInterface            An implicit `StorageInterface` for interacting with cloud storage.
  * @param connectorTaskId             An implicit `ConnectorTaskId` representing the task's unique identifier.
  */
class IndexManagerV2(
  bucketAndPrefixFn:           TopicPartition => Either[SinkError, CloudLocation],
  oldIndexManager:             IndexManagerV1,
  pendingOperationsProcessors: PendingOperationsProcessors,
)(
  implicit
  storageInterface: StorageInterface[?],
  connectorTaskId:  ConnectorTaskId,
) extends IndexManager
    with LazyLogging {

  // A unique identifier for the lock owner, derived from the connector task ID.
  private val lockOwner = connectorTaskId.lockUuid

  // A mutable map that stores the latest offset for each TopicPartition that was seeked during the initialization of the Kafka Connect SinkTask.
  // The key is the TopicPartition and the value is the corresponding Offset.
  private val seekedOffsets = mutable.Map.empty[TopicPartition, Offset]

  // A mutable map that tracks the latest eTags for index files, enabling detection of changes to the files for appropriate handling.
  private val topicPartitionToETags = mutable.Map.empty[TopicPartition, String]

  /**
    * Opens a set of topic partitions for writing. If an index file is not found,
    * a new one is created.
    *
    * @param topicPartitions A set of `TopicPartition` objects to open.
    * @return An `Either` containing a `SinkError` on failure or a map of
    *         `TopicPartition` to `Option[Offset]` on success.
    */
  override def open(topicPartitions: Set[TopicPartition]): Either[SinkError, Map[TopicPartition, Option[Offset]]] =
    topicPartitions.toList
      .parTraverse(tp => EitherT(IO(open(tp))).map(tp -> _))
      .map(_.toMap)
      .value
      .unsafeRunSync()

  /**
    * Opens a single topic partition for writing. If an index file is not found,
    * a new one is created.
    *
    * Pending operations for the topic partition are processed using the
    * `PendingOperationsProcessors` class. The index file is updated after
    * processing to reflect the new state, ensuring that any task picking up
    * the work can resume from the last known state. This mechanism ensures
    * that pending operations are completed before new offsets are processed,
    * maintaining data integrity and consistency.
    *
    * @param topicPartition The `TopicPartition` to open.
    * @return An `Either` containing a `SinkError` on failure or an `Option[Offset]` on success.
    */
  private def open(topicPartition: TopicPartition): Either[SinkError, Option[Offset]] = {

    val path = generateLockFilePath(connectorTaskId, topicPartition)
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      open: ObjectWithETag[IndexFile] <- tryOpen(bucketAndPrefix.bucket, path) match {

        case Left(FileNotFoundError(_, _)) =>
          createNewIndexFile(topicPartition, path, bucketAndPrefix)

        case Left(fileLoadError: FileLoadError) =>
          new FatalCloudSinkError(fileLoadError.message(), fileLoadError.toExceptionOption, topicPartition).asLeft[
            ObjectWithETag[IndexFile],
          ]

        case Right(objectWithetag @ ObjectWithETag(
              idx @ IndexFile(_, committedOffset, Some(PendingState(pendingOffset, pendingOperations))),
              _,
            )) =>
          // file exists
          for {
            newCommittedOffset <- pendingOperationsProcessors.processPendingOperations(
              topicPartition,
              committedOffset,
              PendingState(pendingOffset, pendingOperations),
              update,
            )
            updatedOffsets = objectWithetag.copy(
              wrappedObject = idx.copy(
                pendingState    = Option.empty,
                committedOffset = newCommittedOffset,
              ),
            )
          } yield updatedOffsets

        case Right(objectWithetag @ ObjectWithETag(IndexFile(_, _, _), _)) =>
          objectWithetag.asRight[SinkError]
      }
    } yield updateDataReturnOffset(topicPartition, open)

  }

  /**
    * Updates internal maps with the latest offset and eTag for a topic partition.
    *
    * @param topicPartition The `TopicPartition` being updated.
    * @param open           The `ObjectWithETag` containing the index file and its eTag.
    * @return An `Option[Offset]` representing the committed offset.
    */
  private def updateDataReturnOffset(
    topicPartition: TopicPartition,
    open:           ObjectWithETag[IndexFile],
  ): Option[Offset] = {
    topicPartitionToETags.put(topicPartition, open.eTag)
    open.wrappedObject.committedOffset.foreach(o => seekedOffsets.put(topicPartition, o))
    open.wrappedObject.committedOffset
  }

  /**
    * Creates a new index file for a topic partition.
    *
    * @param topicPartition  The `TopicPartition` for which the index file is created.
    * @param path            The path to the index file.
    * @param bucketAndPrefix The cloud location for the index file.
    * @return An `Either` containing a `SinkError` on failure or the created `ObjectWithETag[IndexFile]` on success.
    */
  private def createNewIndexFile(
    topicPartition:  TopicPartition,
    path:            String,
    bucketAndPrefix: CloudLocation,
  ): Either[SinkError, ObjectWithETag[IndexFile]] =
    for {
      tpo <- oldIndexManager.seekOffsetsForTopicPartition(topicPartition)
      idx = IndexFile(
        lockOwner,
        tpo.map(_.offset),
        Option.empty,
      )

      blobWrite <- storageInterface.writeBlobToFile(bucketAndPrefix.bucket, path, NoOverwriteExistingObject(idx))
        .leftMap { err: UploadError =>
          new FatalCloudSinkError(err.message(), err.toExceptionOption, topicPartition)
        }
    } yield {
      blobWrite
    }

  /**
    * Attempts to open an index file from cloud storage.
    *
    * @param blobBucket The bucket containing the index file.
    * @param blobPath   The path to the index file.
    * @return An `Either` containing a `FileLoadError` on failure or the loaded `ObjectWithETag[IndexFile]` on success.
    */
  private def tryOpen(blobBucket: String, blobPath: String): Either[FileLoadError, ObjectWithETag[IndexFile]] =
    storageInterface.getBlobAsObject[IndexFile](blobBucket, blobPath)

  /**
    * Updates the state for a specific topic partition.
    *
    * @param topicPartition  The `TopicPartition` to update.
    * @param committedOffset An optional committed offset.
    * @param pendingState    An optional pending state.
    * @return An `Either` containing a `SinkError` on failure or an `Option[Offset]` on success.
    */
  override def update(
    topicPartition:  TopicPartition,
    committedOffset: Option[Offset],
    pendingState:    Option[PendingState],
  ): Either[SinkError, Option[Offset]] = {

    val path = generateLockFilePath(connectorTaskId, topicPartition)
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition).leftMap { err =>
        logger.error(s"Failed to get bucket and prefix for $topicPartition: ${err.message()}")
        err
      }
      eTag <- topicPartitionToETags.get(topicPartition).toRight {
        val error = FatalCloudSinkError("Index not found", topicPartition)
        logger.error(s"Failed to get eTag for $topicPartition: ${error.message}")
        error
      }
      index = ObjectWithETag(
        IndexFile(lockOwner, committedOffset, pendingState),
        eTag,
      )
      blobFileWrite <- storageInterface.writeBlobToFile(
        bucketAndPrefix.bucket,
        path,
        index,
      ) match {
        case Left(err: UploadError) =>
          val error = new FatalCloudSinkError(err.message(), err.toExceptionOption, topicPartition)
          logger.error(s"Failed to write blob file for $topicPartition: ${error.message}")
          error.asLeft
        case Right(objectWithEtag) =>
          logger.trace("Updated file : {}", objectWithEtag)
          objectWithEtag.asRight
      }
    } yield updateDataReturnOffset(topicPartition, blobFileWrite)
  }

  /**
    * Retrieves the seeked offset for a specific topic partition.
    *
    * @param topicPartition The `TopicPartition` to retrieve the offset for.
    * @return An `Option[Offset]` containing the seeked offset, or `None` if not available.
    */
  override def getSeekedOffsetForTopicPartition(topicPartition: TopicPartition): Option[Offset] =
    seekedOffsets.get(topicPartition)

  /**
    * Indicates whether indexing is enabled.
    *
    * @return A boolean value indicating that indexing is enabled.  This implementation always returns 'true'.
    */
  override def indexingEnabled: Boolean = true
}

object IndexManagerV2 {

  /**
    * Converts a given connector task ID and topic partition into a lock file path.
    *
    * @param connectorTaskId the ID of the connector task
    * @param topicPartition the topic partition
    * @return the lock file path as a String
    */
  private def generateLockFilePath(connectorTaskId: ConnectorTaskId, topicPartition: TopicPartition): String =
    s".indexes/.locks/${topicPartition.topic}/${topicPartition.partition}.lock"

}
