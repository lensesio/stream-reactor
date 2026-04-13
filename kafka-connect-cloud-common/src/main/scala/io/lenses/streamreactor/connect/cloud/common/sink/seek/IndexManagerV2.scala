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
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerV2._
import io.lenses.streamreactor.connect.cloud.common.sink.seek.deprecated.IndexManagerV1
import io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.FileNotFoundError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer

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
  directoryFileName:           String,
  maxGranularCacheSize:        Int = IndexManagerV2.DefaultMaxGranularCacheSize,
  gcIntervalSeconds:           Int = IndexManagerV2.DefaultGcIntervalSeconds,
  gcBatchSize:                 Int = IndexManagerV2.DefaultGcBatchSize,
)(
  implicit
  storageInterface: StorageInterface[?],
  connectorTaskId:  ConnectorTaskId,
) extends IndexManager
    with LazyLogging {

  // A unique identifier for the lock owner, derived from the connector task ID.
  private val lockOwner = connectorTaskId.lockUuid

  // Thread-safe map storing the latest offset for each TopicPartition seeked during SinkTask initialization.
  // Must be concurrent because open() uses parTraverse to process partitions on multiple fibers.
  private val seekedOffsets = TrieMap.empty[TopicPartition, Offset]

  // Thread-safe map tracking the latest eTags for index files, enabling conditional writes.
  // Must be concurrent because open() uses parTraverse to process partitions on multiple fibers.
  private val topicPartitionToETags = TrieMap.empty[TopicPartition, String]

  // Granular lock cache: keyed by (TopicPartition, partitionKey).
  // Lazily populated on first writer access. Not bounded by automatic eviction — entries are
  // removed by cleanUpObsoleteLocks (GC enqueue), evictAllGranularLocks (shutdown/rebalance),
  // and evictGranularLock (explicit single-key eviction).
  // This avoids the desync where hard LRU eviction removes an entry for an active writer,
  // causing a spurious FatalCloudSinkError on next flush.
  //
  // An entry can have offset = None when the lock file exists in storage but has no committed
  // offset yet (e.g. a freshly created lock via ensureGranularLock).
  //
  // Thread safety: ConcurrentHashMap is required because the background GC thread reads
  // the cache (containsKey) to check whether a scheduled-for-deletion key has been reclaimed
  // by a new writer. All mutating access occurs on the single Kafka Connect task thread.
  private val granularCache = new ConcurrentHashMap[(TopicPartition, String), GranularCacheEntry]()

  // Exposed for testing only; not part of the public API.
  private[seek] def granularCacheSize: Int = granularCache.size()

  private val gcQueue: ConcurrentLinkedQueue[GcItem] = new ConcurrentLinkedQueue()

  private val gcExecutor: ScheduledExecutorService = {
    val executor = Executors.newSingleThreadScheduledExecutor { (r: Runnable) =>
      val t = new Thread(r, s"gc-${connectorTaskId.show}")
      t.setDaemon(true)
      t
    }
    executor.scheduleAtFixedRate(() => drainGcQueue(), gcIntervalSeconds.toLong, gcIntervalSeconds.toLong, TimeUnit.SECONDS)
    executor
  }

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

    val maybeOldPath = generateLockFilePathMigration(connectorTaskId, topicPartition, directoryFileName)
    val path         = generateLockFilePath(connectorTaskId, topicPartition, directoryFileName)
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      _               <- migrateOldPathIfExists(bucketAndPrefix, maybeOldPath, path, topicPartition)
      offset <- tryOpen(bucketAndPrefix.bucket, path) match {

        case Left(FileNotFoundError(_, _)) =>
          createNewIndexFileNoOverwrite(topicPartition, path, bucketAndPrefix)
            .map(updateDataReturnOffset(topicPartition, _))

        case Left(fileLoadError: FileLoadError) =>
          new FatalCloudSinkError(fileLoadError.message(), fileLoadError.toExceptionOption, topicPartition).asLeft[
            Option[Offset],
          ]

        case Right(objectWithetag @ ObjectWithETag(
              IndexFile(_, committedOffset, Some(PendingState(pendingOffset, pendingOperations))),
              _,
            )) =>
          topicPartitionToETags.put(topicPartition, objectWithetag.eTag)
          pendingOperationsProcessors.processPendingOperations(
            topicPartition,
            committedOffset,
            PendingState(pendingOffset, pendingOperations),
            update,
          ).map { resolvedOffset =>
            // processPendingOperations calls update() which already maintains topicPartitionToETags
            // with the correct eTag. We must NOT overwrite it with the stale original eTag.
            // Only ensure seekedOffsets is updated for cases where update() was not called
            // (e.g. cancelPending on the last operation).
            resolvedOffset.foreach(o => seekedOffsets.put(topicPartition, o))
            resolvedOffset
          }

        case Right(objectWithetag @ ObjectWithETag(IndexFile(_, _, _), _)) =>
          updateDataReturnOffset(topicPartition, objectWithetag).asRight[SinkError]
      }
    } yield offset

  }

  private def migrateOldPathIfExists(
    bucketAndPrefix: CloudLocation,
    maybeOldPath:    String,
    path:            String,
    topicPartition:  TopicPartition,
  ): Either[FatalCloudSinkError, Unit] =
    storageInterface.pathExists(bucketAndPrefix.bucket, maybeOldPath) match {
      case Left(error) =>
        //old path does not exist, nothing to do
        val fatalError = new FatalCloudSinkError(error.message(), error.toExceptionOption, topicPartition)
        logger.error(
          s"Failed to check existence of old index file for $topicPartition at $maybeOldPath: ${fatalError.message}",
        )
        fatalError.asLeft
      case Right(false) =>
        //old path does not exist, nothing to do
        ().asRight
      case Right(true) =>
        //old path exists, move to new path
        storageInterface.mvFile(bucketAndPrefix.bucket, maybeOldPath, bucketAndPrefix.bucket, path, None) match {
          case Left(err) =>
            val error = new FatalCloudSinkError(err.message(), err.toExceptionOption, topicPartition)
            logger.error(
              s"Failed to move old index file for $topicPartition from $maybeOldPath to $path: ${error.message}",
            )
            error.asLeft
          case Right(_) =>
            logger.info(s"Migrated old index file for $topicPartition from $maybeOldPath to $path")
            ().asRight
        }
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
   * Creates a new index file for a topic partition based on a previous format index file, or an empty offset if none currently exists.  Will not overwrite an existing index file.
   *
   * @param topicPartition  The `TopicPartition` for which the index file is created.
   * @param path            The path to the index file.
   * @param bucketAndPrefix The cloud location for the index file.
   * @return An `Either` containing a `SinkError` on failure or the created `ObjectWithETag[IndexFile]` on success.
   */
  private def createNewIndexFileNoOverwrite(
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

    val path = generateLockFilePath(connectorTaskId, topicPartition, directoryFileName)
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

  override def getSeekedOffsetForTopicPartition(topicPartition: TopicPartition): Option[Offset] =
    seekedOffsets.get(topicPartition)

  // Cache-first lookup: return the cached offset if present, otherwise fetch the granular lock
  // from cloud storage and populate the cache (lazy load). Returns Right(None) if the lock does
  // not exist in storage yet -- callers fall back to the master lock offset in that case.
  // Returns Left(SinkError) on transient failures so callers can fail-fast rather than
  // silently falling back to a potentially stale master lock offset.
  override def getSeekedOffsetForPartitionKey(
    topicPartition: TopicPartition,
    partitionKey:   String,
  ): Either[SinkError, Option[Offset]] = {
    val cached = granularCache.get((topicPartition, partitionKey))
    if (cached != null) cached.offset.asRight
    else loadGranularLock(topicPartition, partitionKey)
  }

  /**
   * Lazily loads a single granular lock from cloud storage on cache miss.
   * Returns Right(None) if the lock file doesn't exist (FileNotFoundError).
   * Returns Left(SinkError) on transient cloud errors or PendingState resolution failures.
   * If a PendingState is found, it is resolved before caching.
   */
  private def loadGranularLock(
    topicPartition: TopicPartition,
    partitionKey:   String,
  ): Either[SinkError, Option[Offset]] = {
    val path = generateGranularLockFilePath(connectorTaskId, topicPartition, partitionKey, directoryFileName)
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      result <- tryOpen(bucketAndPrefix.bucket, path) match {
        case Right(objectWithEtag @ ObjectWithETag(
              IndexFile(_, committedOffset, Some(PendingState(pendingOffset, pendingOps))),
              _,
            )) =>
          logger.info(s"Lazy-loading granular lock with PendingState for $topicPartition/$partitionKey")
          granularCache.put((topicPartition, partitionKey), GranularCacheEntry(None, objectWithEtag.eTag))
          val fnUpdate: (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]] =
            (tp, co, ps) => updateForPartitionKey(tp, partitionKey, co, ps)
          val result = pendingOperationsProcessors.processPendingOperations(
            topicPartition,
            committedOffset,
            PendingState(pendingOffset, pendingOps),
            fnUpdate,
          )
          if (result.isLeft) {
            granularCache.remove((topicPartition, partitionKey))
          }
          result

        case Right(objectWithEtag @ ObjectWithETag(IndexFile(_, committedOffset, None), _)) =>
          logger.info(s"Lazy-loaded granular lock for $topicPartition/$partitionKey, offset=$committedOffset")
          granularCache.put((topicPartition, partitionKey), GranularCacheEntry(committedOffset, objectWithEtag.eTag))
          committedOffset.asRight

        case Left(_: FileNotFoundError) =>
          Option.empty[Offset].asRight

        case Left(err) =>
          val sinkError: SinkError = new FatalCloudSinkError(
            s"Failed to lazy-load granular lock for $topicPartition/$partitionKey: ${err.message()}",
            err.toExceptionOption,
            topicPartition,
          )
          logger.error(sinkError.message())
          sinkError.asLeft
      }
    } yield result
  }

  /**
   * Resolves the eTag for a granular lock from the in-memory cache.
   *
   * Returns FatalCloudSinkError on cache miss rather than re-reading from storage.
   * Re-reading would defeat the zombie-task fencing mechanism: a zombie whose eTag was
   * LRU-evicted would retrieve the new task's eTag from storage and silently overwrite
   * its lock file, causing data duplication.
   */
  private def resolveGranularETag(
    topicPartition: TopicPartition,
    partitionKey:   String,
  ): Either[SinkError, String] = {
    val cached = granularCache.get((topicPartition, partitionKey))
    if (cached != null) cached.eTag.asRight
    else {
      val error = FatalCloudSinkError(
        s"Granular lock eTag for $topicPartition/$partitionKey not in cache. " +
          s"This may indicate a zombie task whose cache entry was evicted. Failing to preserve fencing.",
        topicPartition,
      )
      logger.error(error.message)
      error.asLeft
    }
  }

  override def updateForPartitionKey(
    topicPartition:  TopicPartition,
    partitionKey:    String,
    committedOffset: Option[Offset],
    pendingState:    Option[PendingState],
  ): Either[SinkError, Option[Offset]] = {
    val path = generateGranularLockFilePath(connectorTaskId, topicPartition, partitionKey, directoryFileName)
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition).leftMap { err =>
        logger.error(s"Failed to get bucket and prefix for $topicPartition: ${err.message()}")
        err
      }
      eTag <- resolveGranularETag(topicPartition, partitionKey)
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
          logger.error(s"Failed to write granular lock for $topicPartition/$partitionKey: ${error.message}")
          error.asLeft
        case Right(objectWithEtag) =>
          logger.trace("Updated granular lock: {}", objectWithEtag)
          objectWithEtag.asRight
      }
    } yield {
      granularCache.put(
        (topicPartition, partitionKey),
        GranularCacheEntry(blobFileWrite.wrappedObject.committedOffset, blobFileWrite.eTag),
      )
      blobFileWrite.wrappedObject.committedOffset
    }
  }

  /**
   * Ensures the granular lock for a partition key has an initial eTag
   * (by creating the lock file if it doesn't already exist).
   *
   * Uses tryOpen instead of pathExists to avoid an extra API call: if the lock already
   * exists, the read populates the cache immediately so that the subsequent
   * getSeekedOffsetForPartitionKey call is a cache hit rather than a second storage read.
   */
  override def ensureGranularLock(
    topicPartition: TopicPartition,
    partitionKey:   String,
  ): Either[SinkError, Unit] =
    if (granularCache.containsKey((topicPartition, partitionKey))) {
      ().asRight
    } else {
      val path = generateGranularLockFilePath(connectorTaskId, topicPartition, partitionKey, directoryFileName)
      for {
        bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
        _ <- tryOpen(bucketAndPrefix.bucket, path) match {
          case Right(objectWithEtag) =>
            granularCache.put(
              (topicPartition, partitionKey),
              GranularCacheEntry(objectWithEtag.wrappedObject.committedOffset, objectWithEtag.eTag),
            )
            ().asRight[SinkError]
          case Left(_: FileNotFoundError) =>
            val idx = IndexFile(lockOwner, None, None)
            storageInterface.writeBlobToFile(
              bucketAndPrefix.bucket,
              path,
              NoOverwriteExistingObject(idx),
            ).map { result =>
              granularCache.put((topicPartition, partitionKey), GranularCacheEntry(None, result.eTag))
              ()
            }.left.flatMap { _: UploadError =>
              // The write failed -- most likely because another task created the file between
              // our read (FileNotFoundError) and this write (NoOverwriteExistingObject
              // precondition failed). Re-read to populate the cache; if the re-read also
              // fails, propagate that error as fatal.
              logger.info(s"NoOverwriteExistingObject write failed for $topicPartition/$partitionKey, " +
                s"re-reading existing lock (likely created by another task)")
              tryOpen(bucketAndPrefix.bucket, path) match {
                case Right(existing) =>
                  granularCache.put(
                    (topicPartition, partitionKey),
                    GranularCacheEntry(existing.wrappedObject.committedOffset, existing.eTag),
                  )
                  ().asRight[SinkError]
                case Left(retryErr) =>
                  (new FatalCloudSinkError(retryErr.message(), retryErr.toExceptionOption, topicPartition): SinkError).asLeft
              }
            }
          case Left(err) =>
            (new FatalCloudSinkError(err.message(), err.toExceptionOption, topicPartition): SinkError).asLeft
        }
      } yield ()
    }

  override def evictGranularLock(
    topicPartition: TopicPartition,
    partitionKey:   String,
  ): Unit = {
    val _ = granularCache.remove((topicPartition, partitionKey))
  }

  override def evictAllGranularLocks(
    topicPartition: TopicPartition,
  ): Unit = {
    val _ = granularCache.keySet().removeIf(_._1 == topicPartition)
  }

  override def updateMasterLock(
    topicPartition:   TopicPartition,
    globalSafeOffset: Offset,
  ): Either[SinkError, Unit] = {
    val path            = generateLockFilePath(connectorTaskId, topicPartition, directoryFileName)
    val committedOffset = if (globalSafeOffset.value > 0) Some(Offset(globalSafeOffset.value - 1)) else None
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      eTag <- topicPartitionToETags.get(topicPartition).toRight {
        FatalCloudSinkError("Master index not found", topicPartition)
      }
      index = ObjectWithETag(
        IndexFile(lockOwner, committedOffset, None),
        eTag,
      )
      blobFileWrite <- storageInterface.writeBlobToFile(
        bucketAndPrefix.bucket,
        path,
        index,
      ) match {
        case Left(err: UploadError) =>
          // Do NOT re-read the master lock to refresh the cached eTag. For transient
          // errors the eTag is still valid and the next cycle will succeed. For eTag
          // mismatches (another task modified the lock) the stale eTag causes repeated
          // failures -- this is correct fencing behavior that prevents a zombie task
          // from overwriting the new task's master lock.
          logger.warn(s"Master lock write failed for $topicPartition (possible fencing by new task): ${err.message()}")
          (new FatalCloudSinkError(err.message(), err.toExceptionOption, topicPartition): SinkError).asLeft
        case Right(objectWithEtag) =>
          objectWithEtag.asRight
      }
    } yield {
      topicPartitionToETags.put(topicPartition, blobFileWrite.eTag)
      blobFileWrite.wrappedObject.committedOffset.foreach(o => seekedOffsets.put(topicPartition, o))
      ()
    }
  }

  override def cleanUpObsoleteLocks(
    topicPartition:     TopicPartition,
    globalSafeOffset:   Offset,
    activePartitionKeys: Set[String],
  ): Either[SinkError, Unit] = {
    val keysToRemove = ListBuffer.empty[(TopicPartition, String)]
    val it = granularCache.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      val (tp, pk) = entry.getKey
      val cached   = entry.getValue
      if (tp == topicPartition &&
        cached.offset.exists(_.value < globalSafeOffset.value) &&
        !activePartitionKeys.contains(pk)) {
        keysToRemove += ((tp, pk))
      }
    }

    if (keysToRemove.isEmpty) return ().asRight

    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
    } yield {
      keysToRemove.toList.foreach {
        case (tp, pk) =>
          granularCache.remove((tp, pk))
          val path = generateGranularLockFilePath(connectorTaskId, tp, pk, directoryFileName)
          gcQueue.add(GcItem(bucketAndPrefix.bucket, path, tp, pk))
      }
      logger.debug(s"Enqueued ${keysToRemove.size} obsolete granular lock(s) for async deletion for $topicPartition")
    }
  }

  // Exposed for testing: triggers one drain cycle synchronously on the calling thread.
  private[seek] def drainGcQueueNow(): Unit = drainGcQueue()

  private def drainGcQueue(): Unit = try {
    val buffer = new ListBuffer[(String, String)]()
    var item   = gcQueue.poll()
    while (item != null) {
      if (granularCache.containsKey((item.topicPartition, item.partitionKey))) {
        logger.debug(s"GC skipping ${item.topicPartition}/${item.partitionKey}: reclaimed by new writer")
      } else {
        buffer += ((item.bucket, item.path))
      }
      item = gcQueue.poll()
    }
    if (buffer.isEmpty) return

    val byBucket: Map[String, Seq[String]] = buffer.toList
      .groupBy(_._1)
      .map { case (bucket, entries) => bucket -> entries.map(_._2) }

    byBucket.foreach {
      case (bucket, paths) =>
        paths.grouped(gcBatchSize).foreach { chunk =>
          storageInterface.deleteFiles(bucket, chunk) match {
            case Left(err) =>
              logger.warn(s"Background GC failed to delete ${chunk.size} lock file(s) from bucket=$bucket: ${err.message()}")
            case Right(_) =>
              logger.debug(s"Background GC deleted ${chunk.size} lock file(s) from bucket=$bucket")
          }
        }
    }
  } catch {
    case e: Exception =>
      logger.warn(s"Unexpected error in background GC drain for ${connectorTaskId.show}", e)
  }

  override def close(): Unit = {
    gcExecutor.shutdownNow()
    try {
      val _ = gcExecutor.awaitTermination(5, TimeUnit.SECONDS)
    } catch {
      case _: InterruptedException => Thread.currentThread().interrupt()
    }
    drainGcQueue()
    logger.info(s"IndexManagerV2 closed for ${connectorTaskId.show}")
  }

  override def indexingEnabled: Boolean = true
}

object IndexManagerV2 {

  case class GranularCacheEntry(offset: Option[Offset], eTag: String)

  private[seek] case class GcItem(bucket: String, path: String, topicPartition: TopicPartition, partitionKey: String)

  // Controls the idle-writer eviction threshold in WriterManager. When the total number of
  // writers exceeds this value, idle (NoWriter-state) writers are evicted along with their
  // granular cache entries. Active writers are never evicted, so both the writers map and
  // granular cache may temporarily exceed this limit during high-cardinality bursts.
  val DefaultMaxGranularCacheSize: Int = 10000
  val DefaultGcIntervalSeconds: Int    = 300
  val DefaultGcBatchSize: Int          = 1000

  /**
   * Converts a given connector task ID and topic partition into a lock file path.
   *
   * @param connectorTaskId the ID of the connector task
   * @param topicPartition the topic partition
   * @return the lock file path as a String
   */
  private[seek] def generateLockFilePath(
    connectorTaskId:   ConnectorTaskId,
    topicPartition:    TopicPartition,
    directoryFileName: String,
  ): String =
    s"$directoryFileName/${connectorTaskId.name}/.locks/${topicPartition.topic}/${topicPartition.partition}.lock"

  /**
   * It is used to migrate 9.0.0 to 10.0.0 regression where connector name is not used in the path
   * allowing scenarios where connectors reading from the same topic overlaps and corrupts state.
   * It is done to avoid manual migration, and used in the open method of IndexManagerV2 only.
   */
  private def generateLockFilePathMigration(
    connectorTaskId:   ConnectorTaskId,
    topicPartition:    TopicPartition,
    directoryFileName: String,
  ): String =
    s"$directoryFileName/.locks/${topicPartition.topic}/${topicPartition.partition}.lock"

  private[seek] def generateGranularLockFilePath(
    connectorTaskId:   ConnectorTaskId,
    topicPartition:    TopicPartition,
    partitionKey:      String,
    directoryFileName: String,
  ): String =
    s"$directoryFileName/${connectorTaskId.name}/.locks/${topicPartition.topic}/${topicPartition.partition}/$partitionKey.lock"

}
