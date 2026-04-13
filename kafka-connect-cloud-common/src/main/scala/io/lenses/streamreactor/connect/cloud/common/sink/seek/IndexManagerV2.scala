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
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerV2._
import io.lenses.streamreactor.connect.cloud.common.sink.seek.deprecated.IndexManagerV1
import io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.FileNotFoundError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import scala.collection.concurrent.TrieMap
import scala.util.Random
import scala.util.control.NonFatal
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
  gcIntervalSeconds:           Int              = IndexManagerV2.DefaultGcIntervalSeconds,
  gcBatchSize:                 Int              = IndexManagerV2.DefaultGcBatchSize,
  gcSweepEnabled:              Boolean          = IndexManagerV2.DefaultGcSweepEnabled,
  gcSweepIntervalSeconds:      Int              = IndexManagerV2.DefaultGcSweepIntervalSeconds,
  gcSweepMinAgeSeconds:        Int              = IndexManagerV2.DefaultGcSweepMinAgeSeconds,
  gcSweepMaxReads:             Int              = IndexManagerV2.DefaultGcSweepMaxReads,
  metrics:                     CloudSinkMetrics = new CloudSinkMetrics(0),
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

  // Granular lock cache: nested ConcurrentHashMap keyed by TopicPartition, then partitionKey.
  // Lazily populated on first writer access. Not bounded by automatic eviction — entries are
  // removed by cleanUpObsoleteLocks (GC enqueue), evictAllGranularLocks (shutdown/rebalance),
  // and evictGranularLock (explicit single-key eviction).
  //
  // Nested structure enables O(keys-in-partition) scans in cleanUpObsoleteLocks instead of
  // O(total-cache-size), avoiding CPU spikes at high partition * key cardinality.
  //
  // Thread safety: ConcurrentHashMap is required because the background GC thread reads
  // the cache (containsKey) to check whether a scheduled-for-deletion key has been reclaimed
  // by a new writer. All mutating access occurs on the single Kafka Connect task thread.
  private val granularCache = new ConcurrentHashMap[TopicPartition, ConcurrentHashMap[String, GranularCacheEntry]]()

  private def gcGet(tp: TopicPartition, pk: String): GranularCacheEntry = {
    val inner = granularCache.get(tp)
    if (inner == null) null else inner.get(pk)
  }

  private def gcPut(tp: TopicPartition, pk: String, entry: GranularCacheEntry): Unit = {
    granularCache.computeIfAbsent(tp, _ => new ConcurrentHashMap[String, GranularCacheEntry]()).put(pk, entry)
    ()
  }

  private def gcRemove(tp: TopicPartition, pk: String): Unit = {
    val inner = granularCache.get(tp)
    if (inner != null) {
      inner.remove(pk)
      if (inner.isEmpty) { val _ = granularCache.remove(tp) }
    }
  }

  private def gcContainsKey(tp: TopicPartition, pk: String): Boolean = {
    val inner = granularCache.get(tp)
    inner != null && inner.containsKey(pk)
  }

  private def gcRemoveAllForTp(tp: TopicPartition): Unit = {
    val _ = granularCache.remove(tp)
  }

  // Exposed for testing only; not part of the public API.
  private[seek] def granularCacheSize: Int =
    granularCache.values().stream().mapToInt(_.size()).sum()

  private val gcQueue: ConcurrentLinkedQueue[GcItem] = new ConcurrentLinkedQueue()

  private val gcExecutor: ScheduledExecutorService = {
    val executor = Executors.newSingleThreadScheduledExecutor { (r: Runnable) =>
      val t = new Thread(r, s"gc-${connectorTaskId.show}")
      t.setDaemon(true)
      t
    }
    executor.scheduleAtFixedRate(() => drainGcQueue(),
                                 gcIntervalSeconds.toLong,
                                 gcIntervalSeconds.toLong,
                                 TimeUnit.SECONDS,
    )
    executor
  }

  private val sweepExecutor: Option[ScheduledExecutorService] =
    Option.when(gcSweepEnabled) {
      val executor = Executors.newSingleThreadScheduledExecutor { (r: Runnable) =>
        val t = new Thread(r, s"sweep-${connectorTaskId.show}")
        t.setDaemon(true)
        t
      }
      executor.scheduleAtFixedRate(() => sweepOrphanedLocks(),
                                   gcSweepIntervalSeconds.toLong,
                                   gcSweepIntervalSeconds.toLong,
                                   TimeUnit.SECONDS,
      )
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
    val cached = gcGet(topicPartition, partitionKey)
    if (cached != null) {
      metrics.incrementGranularCacheHits()
      cached.offset.asRight
    } else {
      metrics.incrementGranularCacheMisses()
      loadGranularLock(topicPartition, partitionKey)
    }
  }

  /**
   * Retrieves the seeked offset for a specific topic partition.
   * Lazily loads a single granular lock from cloud storage on cache miss.
   * Returns Right(None) if the lock file doesn't exist (FileNotFoundError).
   * Returns Left(SinkError) on transient cloud errors or PendingState resolution failures.
   *
   * If a PendingState is found, it is resolved (completed or rolled back) **before** the
   * offset and eTag are cached. A PendingState means the previous task instance crashed
   * mid-commit, between phases of the Upload → Copy → Delete protocol. The lock file
   * records which operations were still in flight. Resolution cannot be deferred for two
   * reasons:
   *
   *  - '''Offset indeterminacy''': The `committedOffset` stored alongside a PendingState
   *    reflects the state ''before'' the interrupted commit. If the pending operations
   *    complete successfully, the true committed offset advances to `pendingOffset`. If
   *    they are cancelled (e.g. the staging file no longer exists), it stays at
   *    `committedOffset`. The writer's `shouldSkip` logic needs the resolved offset to
   *    correctly deduplicate records -- caching the pre-resolution value would cause
   *    duplication (offset too low: already-committed records not skipped) or data loss
   *    (offset too high after a stale cache hit: uncommitted records skipped).
   *
   *  - '''eTag staleness''': Each step of `processPendingOperations` writes an updated
   *    lock file (recording progress or clearing the pending state), advancing the eTag.
   *    If we cached the pre-resolution eTag and let a writer proceed, its next conditional
   *    write would fail with an eTag mismatch. Worse, if resolution ran later on a
   *    different code path, the writer's cached eTag would be stale, breaking the
   *    zombie-fencing invariant (see "Zombie task and temp-upload fencing" in the
   *    architecture doc).
   *
   * On resolution failure the cache entry is removed so that a subsequent access retries
   * cleanly from storage.
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
          gcPut(topicPartition, partitionKey, GranularCacheEntry(None, objectWithEtag.eTag))
          val fnUpdate: (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]] =
            (tp, co, ps) => updateForPartitionKey(tp, partitionKey, co, ps)
          val result = pendingOperationsProcessors.processPendingOperations(
            topicPartition,
            committedOffset,
            PendingState(pendingOffset, pendingOps),
            fnUpdate,
          )
          if (result.isLeft) {
            gcRemove(topicPartition, partitionKey)
          }
          result

        case Right(objectWithEtag @ ObjectWithETag(IndexFile(_, committedOffset, None), _)) =>
          logger.info(s"Lazy-loaded granular lock for $topicPartition/$partitionKey, offset=$committedOffset")
          gcPut(topicPartition, partitionKey, GranularCacheEntry(committedOffset, objectWithEtag.eTag))
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
    val cached = gcGet(topicPartition, partitionKey)
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
      gcPut(topicPartition,
            partitionKey,
            GranularCacheEntry(blobFileWrite.wrappedObject.committedOffset, blobFileWrite.eTag),
      )
      metrics.setGranularCacheSize(granularCacheSize)
      blobFileWrite.wrappedObject.committedOffset
    }
  }

  /**
   * Caches a granular lock read from storage, resolving any PendingState first.
   * When PendingState is present the pending upload/copy/delete operations are
   * completed (or rolled back) via processPendingOperations before the resolved
   * offset is cached. This mirrors the handling in loadGranularLock.
   */
  private def resolveAndCacheGranularLock(
    topicPartition: TopicPartition,
    partitionKey:   String,
    objectWithEtag: ObjectWithETag[IndexFile],
  ): Either[SinkError, Unit] =
    objectWithEtag match {
      case ObjectWithETag(IndexFile(_, committedOffset, Some(PendingState(pendingOffset, pendingOps))), _) =>
        logger.info(s"ensureGranularLock found PendingState for $topicPartition/$partitionKey, resolving")
        gcPut(topicPartition, partitionKey, GranularCacheEntry(None, objectWithEtag.eTag))
        val fnUpdate: (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]] =
          (tp, co, ps) => updateForPartitionKey(tp, partitionKey, co, ps)
        val result = pendingOperationsProcessors.processPendingOperations(
          topicPartition,
          committedOffset,
          PendingState(pendingOffset, pendingOps),
          fnUpdate,
        )
        if (result.isLeft) {
          gcRemove(topicPartition, partitionKey)
        }
        result.map(_ => ())

      case ObjectWithETag(IndexFile(_, committedOffset, None), eTag) =>
        gcPut(topicPartition, partitionKey, GranularCacheEntry(committedOffset, eTag))
        ().asRight
    }

  /**
   * Ensures the granular lock for a partition key has an initial eTag
   * (by creating the lock file if it doesn't already exist).
   *
   * Uses tryOpen instead of pathExists to avoid an extra API call: if the lock already
   * exists, the read populates the cache immediately so that the subsequent
   * getSeekedOffsetForPartitionKey call is a cache hit rather than a second storage read.
   *
   * If the existing lock contains a PendingState (from a crash mid-commit),
   * the pending operations are resolved before caching.
   */
  override def ensureGranularLock(
    topicPartition: TopicPartition,
    partitionKey:   String,
  ): Either[SinkError, Unit] =
    if (gcContainsKey(topicPartition, partitionKey)) {
      metrics.incrementGranularCacheHits()
      ().asRight
    } else {
      metrics.incrementGranularCacheMisses()
      val path = generateGranularLockFilePath(connectorTaskId, topicPartition, partitionKey, directoryFileName)
      for {
        bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
        _ <- tryOpen(bucketAndPrefix.bucket, path) match {
          case Right(objectWithEtag) =>
            resolveAndCacheGranularLock(topicPartition, partitionKey, objectWithEtag)
          case Left(_: FileNotFoundError) =>
            val idx = IndexFile(lockOwner, None, None)
            storageInterface.writeBlobToFile(
              bucketAndPrefix.bucket,
              path,
              NoOverwriteExistingObject(idx),
            ).map { result =>
              gcPut(topicPartition, partitionKey, GranularCacheEntry(None, result.eTag))
              ()
            }.left.flatMap { _: UploadError =>
              // The write failed -- most likely because another task created the file between
              // our read (FileNotFoundError) and this write (NoOverwriteExistingObject
              // precondition failed). Re-read to populate the cache; if the re-read also
              // fails, propagate that error as fatal.
              logger.info(
                s"NoOverwriteExistingObject write failed for $topicPartition/$partitionKey, " +
                  s"re-reading existing lock (likely created by another task)",
              )
              tryOpen(bucketAndPrefix.bucket, path) match {
                case Right(existing) =>
                  resolveAndCacheGranularLock(topicPartition, partitionKey, existing)
                case Left(retryErr) =>
                  (new FatalCloudSinkError(retryErr.message(),
                                           retryErr.toExceptionOption,
                                           topicPartition,
                  ): SinkError).asLeft
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
    gcRemove(topicPartition, partitionKey)
    metrics.setGranularCacheSize(granularCacheSize)
  }

  override def evictAllGranularLocks(
    topicPartition: TopicPartition,
  ): Unit = {
    gcRemoveAllForTp(topicPartition)
    metrics.setGranularCacheSize(granularCacheSize)
  }

  override def clearTopicPartitionState(topicPartition: TopicPartition): Unit = {
    seekedOffsets.remove(topicPartition)
    val _ = topicPartitionToETags.remove(topicPartition)
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
    topicPartition:      TopicPartition,
    globalSafeOffset:    Offset,
    activePartitionKeys: Set[String],
  ): Either[SinkError, Unit] = {
    val inner = granularCache.get(topicPartition)
    if (inner == null || inner.isEmpty) return ().asRight

    val keysToRemove = ListBuffer.empty[String]
    val it           = inner.entrySet().iterator()
    while (it.hasNext) {
      val entry  = it.next()
      val pk     = entry.getKey
      val cached = entry.getValue
      if (
        cached.offset.exists(_.value < globalSafeOffset.value) &&
        !activePartitionKeys.contains(pk)
      ) {
        keysToRemove += pk
      }
    }

    if (keysToRemove.isEmpty) return ().asRight

    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
    } yield {
      keysToRemove.toList.foreach { pk =>
        gcRemove(topicPartition, pk)
        val path = generateGranularLockFilePath(connectorTaskId, topicPartition, pk, directoryFileName)
        gcQueue.add(GcItem(bucketAndPrefix.bucket, path, topicPartition, pk))
      }
      metrics.incrementGcLocksEnqueued(keysToRemove.size.toLong)
      metrics.setGranularCacheSize(granularCacheSize)
      metrics.setGcQueueDepth(gcQueue.size())
      logger.debug(s"Enqueued ${keysToRemove.size} obsolete granular lock(s) for async deletion for $topicPartition")
    }
  }

  private[seek] def drainGcQueue(): Unit =
    try {
      val eligible = new ListBuffer[GcItem]()
      var item     = gcQueue.poll()
      while (item != null) {
        if (!seekedOffsets.contains(item.topicPartition)) {
          logger.debug(
            s"GC discarding ${item.topicPartition}/${item.partitionKey}: partition no longer owned by this task",
          )
          metrics.incrementGcLocksSkippedRevoked()
        } else if (gcContainsKey(item.topicPartition, item.partitionKey)) {
          logger.debug(s"GC skipping ${item.topicPartition}/${item.partitionKey}: reclaimed by new writer")
          metrics.incrementGcLocksSkippedReclaimed()
        } else {
          eligible += item
        }
        item = gcQueue.poll()
      }
      metrics.setGcQueueDepth(gcQueue.size())
      if (eligible.isEmpty) return

      val byBucket: Map[String, Seq[GcItem]] = eligible.toList.groupBy(_.bucket)

      byBucket.foreach {
        case (bucket, items) =>
          items.grouped(gcBatchSize).foreach { chunk =>
            storageInterface.deleteFiles(bucket, chunk.map(_.path)) match {
              case Left(err) =>
                metrics.incrementGcDeleteFailures()
                logger.warn(
                  s"Background GC failed to delete ${chunk.size} lock file(s) from bucket=$bucket: ${err.message()}",
                )
                val retryable = chunk.filter(_.retryCount < MaxGcRetries)
                retryable.foreach(i => gcQueue.add(i.copy(retryCount = i.retryCount + 1)))
                if (retryable.nonEmpty) {
                  metrics.incrementGcDeleteRetries(retryable.size.toLong)
                  logger.debug(
                    s"Re-enqueued ${retryable.size} item(s) for retry (dropped ${chunk.size - retryable.size} that exceeded max retries)",
                  )
                }
              case Right(_) =>
                metrics.incrementGcLocksDeleted(chunk.size.toLong)
                logger.debug(s"Background GC deleted ${chunk.size} lock file(s) from bucket=$bucket")
            }
          }
      }
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Unexpected error in background GC drain for ${connectorTaskId.show}", e)
    }

  private[seek] def sweepOrphanedLocks(): Unit =
    try {
      if (!gcSweepEnabled) return

      metrics.incrementSweepRuns()
      var readsRemaining = gcSweepMaxReads
      var totalEnqueued  = 0
      var tpsScanned     = 0
      var tpsSkipped     = 0
      val ageThreshold   = Instant.now().minusSeconds(gcSweepMinAgeSeconds.toLong)
      val now            = System.currentTimeMillis()

      for (tp <- Random.shuffle(seekedOffsets.keys.toList) if readsRemaining > 0) {
        seekedOffsets.get(tp).foreach { masterOffset =>
          bucketAndPrefixFn(tp) match {
            case Right(loc) =>
              val bucket = loc.bucket
              if (isSweepDueForPartition(bucket, tp, now)) {
                writeSweepMarkerForPartition(bucket, tp, now)
                tpsScanned += 1
                val (enqueued, readsUsed) = sweepPartition(tp, masterOffset, ageThreshold, readsRemaining)
                totalEnqueued += enqueued
                readsRemaining -= readsUsed
              } else {
                tpsSkipped += 1
              }
            case Left(_) => ()
          }
        }
      }

      metrics.incrementSweepOrphansEnqueued(totalEnqueued.toLong)
      metrics.setSweepGetBudgetUsed(gcSweepMaxReads - readsRemaining)

      if (totalEnqueued > 0 || tpsScanned > 0) {
        logger.info(
          s"Orphan sweep complete for ${connectorTaskId.show}: scanned=$tpsScanned TPs, " +
            s"skipped=$tpsSkipped TPs (marker not expired), enqueued=$totalEnqueued orphans, " +
            s"remaining GET budget=$readsRemaining",
        )
      }
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Unexpected error in orphan sweep for ${connectorTaskId.show}", e)
    }

  /** Checks whether the sweep marker for a specific TopicPartition has expired. */
  private def isSweepDueForPartition(bucket: String, tp: TopicPartition, now: Long): Boolean = {
    val markerPath = generateSweepMarkerPath(connectorTaskId, tp, directoryFileName)
    storageInterface.getBlobAsObject[SweepMarker](bucket, markerPath) match {
      case Right(ObjectWithETag(marker, _)) if marker.nextRunEpochMillis > now =>
        false
      case Left(err) if !err.isInstanceOf[FileNotFoundError] =>
        logger.warn(s"Transient error reading sweep marker for $tp in bucket=$bucket, skipping: ${err.message()}")
        false
      case _ =>
        true
    }
  }

  /** Writes a sweep marker for a specific TopicPartition (write-before-sweep pattern). */
  private def writeSweepMarkerForPartition(bucket: String, tp: TopicPartition, now: Long): Unit = {
    val markerPath = generateSweepMarkerPath(connectorTaskId, tp, directoryFileName)
    val markerJson = {
      import io.circe.syntax._
      import IndexManagerV2.SweepMarker.sweepMarkerEncoder
      SweepMarker(now, now + gcSweepIntervalSeconds * 1000L).asJson.noSpaces
    }
    val uploadable = io.lenses.streamreactor.connect.cloud.common.model.UploadableString(markerJson)
    storageInterface.writeStringToFile(bucket, markerPath, uploadable) match {
      case Left(err) =>
        logger.warn(s"Failed to write sweep marker for $tp to bucket=$bucket: $err")
      case Right(_) => ()
    }
  }

  /** Lists lock files for one TopicPartition, classifies each, and enqueues orphans into gcQueue. */
  private def sweepPartition(
    tp:             TopicPartition,
    masterOffset:   Offset,
    ageThreshold:   Instant,
    readsRemaining: Int,
  ): (Int, Int) = {
    val prefix = s"$directoryFileName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/"
    val bucket = bucketAndPrefixFn(tp) match {
      case Right(loc) => loc.bucket
      case Left(_)    => return (0, 0)
    }

    val files = storageInterface.listFileMetaRecursive(bucket, Some(prefix)) match {
      case Left(err) =>
        logger.warn(s"Sweep: failed to list files for $tp: ${err.message()}")
        return (0, 0)
      case Right(None) =>
        return (0, 0)
      case Right(Some(listing)) =>
        // Safe: SM <: FileMetadata and Seq is covariant, but the existential type on
        // StorageInterface[?] prevents the compiler from proving it. The cast is always valid.
        listing.files.asInstanceOf[Seq[FileMetadata]]
    }

    var budget   = readsRemaining
    var enqueued = 0
    for (fileMeta <- files if budget > 0) {
      classifyLockFile(tp, fileMeta, ageThreshold) match {
        case SweepSkip => // no-op
        case SweepNeedsRead(partitionKey) =>
          budget -= 1
          if (readAndEnqueue(bucket, fileMeta.file, tp, partitionKey, masterOffset))
            enqueued += 1
      }
    }
    (enqueued, readsRemaining - budget)
  }

  private sealed trait SweepClassification
  private case object SweepSkip extends SweepClassification
  private case class SweepNeedsRead(partitionKey: String) extends SweepClassification

  /** Applies extension, recency, and cache-presence filters to decide whether a lock file needs a GET read. */
  private def classifyLockFile(
    tp:           TopicPartition,
    fileMeta:     FileMetadata,
    ageThreshold: Instant,
  ): SweepClassification = {
    val path = fileMeta.file
    if (!path.endsWith(".lock")) return SweepSkip
    if (fileMeta.lastModified.isAfter(ageThreshold)) return SweepSkip

    val fileName     = path.substring(path.lastIndexOf('/') + 1)
    val partitionKey = fileName.stripSuffix(".lock")
    if (gcContainsKey(tp, partitionKey)) SweepSkip
    else SweepNeedsRead(partitionKey)
  }

  /**
   * GETs the lock file and enqueues it for deletion if its committedOffset is below the master.
   * Lock files with PendingState are also safe to sweep when their committedOffset is below master:
   * the master lock can only advance past an offset once all writers have committed it.
   */
  private def readAndEnqueue(
    bucket:       String,
    path:         String,
    tp:           TopicPartition,
    partitionKey: String,
    masterOffset: Offset,
  ): Boolean =
    storageInterface.getBlobAsObject[IndexFile](bucket, path) match {
      case Left(err) =>
        logger.warn(s"Sweep: failed to read lock file $path: ${err.message()}")
        false
      case Right(ObjectWithETag(IndexFile(_, Some(committedOffset), _), _))
          if committedOffset.value < masterOffset.value =>
        gcQueue.add(GcItem(bucket, path, tp, partitionKey))
        true
      case _ =>
        false
    }

  override def close(): Unit = {
    sweepExecutor.foreach { exec =>
      exec.shutdownNow()
      try {
        val _ = exec.awaitTermination(5, TimeUnit.SECONDS)
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    }
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

  private[seek] case class GcItem(
    bucket:         String,
    path:           String,
    topicPartition: TopicPartition,
    partitionKey:   String,
    retryCount:     Int = 0,
  )

  val MaxGcRetries: Int = 3

  private[seek] case class SweepMarker(lastRunEpochMillis: Long, nextRunEpochMillis: Long)

  private[seek] object SweepMarker {
    import io.circe.generic.semiauto._
    implicit val sweepMarkerEncoder: io.circe.Encoder[SweepMarker] = deriveEncoder
    implicit val sweepMarkerDecoder: io.circe.Decoder[SweepMarker] = deriveDecoder
  }

  val DefaultGcIntervalSeconds:      Int     = 300
  val DefaultGcBatchSize:            Int     = 1000
  val DefaultGcSweepEnabled:         Boolean = true
  val DefaultGcSweepIntervalSeconds: Int     = 86400
  val DefaultGcSweepMinAgeSeconds:   Int     = 86400
  val DefaultGcSweepMaxReads:        Int     = 1000

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

  private[seek] def generateSweepMarkerPath(
    connectorTaskId:   ConnectorTaskId,
    topicPartition:    TopicPartition,
    directoryFileName: String,
  ): String =
    s"$directoryFileName/${connectorTaskId.name}/.locks/${topicPartition.topic}/${topicPartition.partition}/sweep-marker.json"

}
