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

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.seek.deprecated.IndexManagerV1
import io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError

import scala.collection.mutable

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

  private val lockOwner = connectorTaskId.lockUuid

  // A mutable map that stores the latest offset for each TopicPartition that was seeked during the initialization of the Kafka Connect SinkTask.
  // The key is the TopicPartition and the value is the corresponding Offset.
  private val seekedOffsets = mutable.Map.empty[TopicPartition, Offset]

  private val topicPartitionToETags = mutable.Map.empty[TopicPartition, String]

  /**
    * Opens a topic/partition for writing
    * If an index file hasn't been found, we create one.
    *
    * @param topicPartition
    * @return
    */
  override def open(topicPartitions: Set[TopicPartition]): Either[SinkError, Map[TopicPartition, Option[Offset]]] =
    topicPartitions.toList.traverse(tp => open(tp).map(tp -> _)).map(_.toMap)

  /**
    * Opens a topic/partition for writing
    * If an index file hasn't been found, we create one.
    *
    * @param topicPartition
    * @return
    */
  private def open(topicPartition: TopicPartition): Either[SinkError, Option[Offset]] = {

    val path = convertTopicPartitionToPath(connectorTaskId, topicPartition)
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      open: ObjectWithETag[IndexFile] <- tryOpen(bucketAndPrefix.bucket, path) match {
        case Left(s @ FileLoadError(ex, _, false)) =>
          new FatalCloudSinkError(s.message(), ex.some, topicPartition).asLeft[ObjectWithETag[IndexFile]]

        case Left(FileLoadError(_, _, true)) =>
          createNewIndexFile(topicPartition, path, bucketAndPrefix)

        case Right(objectWithetag @ ObjectWithETag(
              idx @ IndexFile(_, committedOffset, Some(PendingState(pendingOffset, pendingOperations))),
              _,
            )) =>
          // file exists
          for {
            processPending <- pendingOperationsProcessors.processPendingOperations(
              topicPartition,
              committedOffset,
              PendingState(pendingOffset, pendingOperations),
              update,
            )
            updatedOffsets = objectWithetag.copy(
              wrappedObject = idx.copy(
                pendingState    = Option.empty,
                committedOffset = processPending,
              ),
            )
          } yield updatedOffsets

        case Right(objectWithetag @ ObjectWithETag(IndexFile(_, _, _), _)) =>
          objectWithetag.asRight[SinkError]
      }
    } yield updateDataReturnOffset(topicPartition, open)

  }

  private def updateDataReturnOffset(
    topicPartition: TopicPartition,
    open:           ObjectWithETag[IndexFile],
  ): Option[Offset] = {
    topicPartitionToETags.put(topicPartition, open.eTag)
    open.wrappedObject.committedOffset.foreach(o => seekedOffsets.put(topicPartition, o))
    open.wrappedObject.committedOffset
  }

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

  private def tryOpen(blobBucket: String, blobPath: String): Either[FileLoadError, ObjectWithETag[IndexFile]] =
    for {
      indexFile <- storageInterface.getBlobAsObject[IndexFile](blobBucket, blobPath)
    } yield indexFile

  override def update(
    topicPartition:  TopicPartition,
    committedOffset: Option[Offset],
    pendingState:    Option[PendingState],
  ): Either[SinkError, Option[Offset]] = {

    val path = convertTopicPartitionToPath(connectorTaskId, topicPartition)
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
    * Converts a given connector task ID and topic partition into a lock file path.
    *
    * @param connectorTaskId the ID of the connector task
    * @param topicPartition the topic partition
    * @return the lock file path as a String
    */
  private def convertTopicPartitionToPath(connectorTaskId: ConnectorTaskId, topicPartition: TopicPartition): String =
    s".indexes/.locks/${topicPartition.topic}/${topicPartition.partition}.lock"

  override def getSeekedOffsetForTopicPartition(topicPartition: TopicPartition): Option[Offset] =
    seekedOffsets.get(topicPartition)

  override def indexingEnabled: Boolean = true
}
