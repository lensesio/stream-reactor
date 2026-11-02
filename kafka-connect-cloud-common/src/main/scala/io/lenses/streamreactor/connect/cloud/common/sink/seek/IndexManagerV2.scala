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
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerV2._
import io.lenses.streamreactor.connect.cloud.common.storage.EmptyFileError
import io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.FileNotFoundError
import io.lenses.streamreactor.connect.cloud.common.storage.NonOverwriteFileExistsError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.util.control.NonFatal

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
 * @param bucketAndPrefixFn           A function that maps a `TopicPartition` to an `Either` containing
 *                                    a `SinkError` or a `CloudLocation`.
 * @param pendingOperationsProcessors A processor for handling pending operations.
 * @param storageInterface            An implicit `StorageInterface` for interacting with cloud storage.
 * @param connectorTaskId             An implicit `ConnectorTaskId` representing the task's unique identifier.
 */
class IndexManagerV2(
  bucketAndPrefixFn:            TopicPartition => Either[SinkError, CloudLocation],
  pendingOperationsProcessors:  PendingOperationsProcessors,
  directoryFileName:            String,
  gcIntervalSeconds:            Int              = IndexManagerV2.DefaultGcIntervalSeconds,
  gcBatchSize:                  Int              = IndexManagerV2.DefaultGcBatchSize,
  gcSweepEnabled:               Boolean          = IndexManagerV2.DefaultGcSweepEnabled,
  gcSweepIntervalSeconds:       Int              = IndexManagerV2.DefaultGcSweepIntervalSeconds,
  gcSweepMinAgeSeconds:         Int              = IndexManagerV2.DefaultGcSweepMinAgeSeconds,
  gcSweepMaxReads:              Int              = IndexManagerV2.DefaultGcSweepMaxReads,
  private[sink] val commitMode: CommitMode       = CommitMode.Default,
  metrics:                      CloudSinkMetrics = new CloudSinkMetrics(),
)(
  implicit
  storageInterface: StorageInterface[?],
  connectorTaskId:  ConnectorTaskId,
) extends IndexManager
    with LazyLogging {

  // Construction-time validation: the orphan sweep's age threshold (gcSweepMinAgeSeconds) must be
  // greater than or equal to the sweep's tick interval (gcSweepIntervalSeconds). Otherwise a lock
  // file written shortly before a sweep tick fires could be misclassified as orphaned and reaped
  // before the writer has had a chance to claim it. The previous behaviour was to silently accept
  // an inverted configuration; closing the documented gap pre-validates and fails fast.
  require(
    gcSweepMinAgeSeconds >= gcSweepIntervalSeconds,
    s"gcSweepMinAgeSeconds ($gcSweepMinAgeSeconds) must be >= gcSweepIntervalSeconds ($gcSweepIntervalSeconds)",
  )

  private val lockOwner = connectorTaskId.lockUuid

  // Thread-safe map storing the latest offset for each TopicPartition seeked during SinkTask initialization.
  // Must be concurrent because open() uses parTraverse to process partitions on multiple fibers.
  private val seekedOffsets = TrieMap.empty[TopicPartition, Offset]

  // Thread-safe map tracking the latest eTags for index files, enabling conditional writes.
  // Must be concurrent because open() uses parTraverse to process partitions on multiple fibers.
  // Visibility: private[seek] so tests in this package can observe eTag-cache invariants.
  private[seek] val topicPartitionToETags = TrieMap.empty[TopicPartition, String]

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
  private val granularCache            = new ConcurrentHashMap[TopicPartition, ConcurrentHashMap[String, GranularCacheEntry]]()
  private val granularCacheSizeCounter = new AtomicInteger(0)

  // Per-TopicPartition record of whether this task instance has confirmed that no legacy
  // granular locks remain (all fenced/resolved and deleted, or none ever existed). Batch mode
  // only. `batchDedupFloor` never consults this -- the dedup floor is master-only (see its
  // doc) -- it exists purely to short-circuit the LIST + fencing work in `snapshotLegacyLocks`
  // on subsequent `open()` calls within the same task lifetime.
  private val legacyResolved = TrieMap.empty[TopicPartition, Boolean]

  // Exposed for tests: has this TP's legacy-lock migration been fenced/resolved and purged?
  private[seek] def legacyPurged(tp: TopicPartition): Boolean =
    legacyResolved.getOrElse(tp, false)

  private def gcGet(tp: TopicPartition, pk: String): Option[GranularCacheEntry] =
    Option(granularCache.get(tp)).flatMap(inner => Option(inner.get(pk)))

  private def gcPut(tp: TopicPartition, pk: String, entry: GranularCacheEntry): Unit = {
    val _ = granularCache.compute(
      tp,
      (_, inner) => {
        val map =
          if (inner == null) new ConcurrentHashMap[String, GranularCacheEntry]() else inner
        val prev = map.put(pk, entry)
        if (prev == null) granularCacheSizeCounter.incrementAndGet()
        map
      },
    )
  }

  private def gcRemove(tp: TopicPartition, pk: String): Unit = {
    val _ = granularCache.compute(
      tp,
      (_, inner) =>
        if (inner == null) null
        else {
          val prev = inner.remove(pk)
          if (prev != null) granularCacheSizeCounter.decrementAndGet()
          if (inner.isEmpty) null else inner
        },
    )
  }

  private def gcContainsKey(tp: TopicPartition, pk: String): Boolean =
    Option(granularCache.get(tp)).exists(_.containsKey(pk))

  private def gcRemoveAllForTp(tp: TopicPartition): Unit = {
    val removed = granularCache.remove(tp)
    if (removed != null) { val _ = granularCacheSizeCounter.addAndGet(-removed.size()) }
  }

  // Exposed for testing only; not part of the public API.
  private[seek] def granularCacheSize: Int = granularCacheSizeCounter.get()

  private val gcQueue: ConcurrentLinkedQueue[GcItem] = new ConcurrentLinkedQueue()

  // Exposed for testing only; not part of the public API.
  private[seek] def gcQueueSize: Int = gcQueue.size()

  // Best-effort gate: when false, scheduled invocations of drainGcQueue / sweepOrphanedLocks
  // are skipped. Set to false by suspendBackgroundWork() (called from CloudSinkTask.close()),
  // set to true at the end of open(). This cannot stop an already-running invocation -- the
  // volatile provides visibility, not mutual exclusion. In-flight executions are benign:
  //  - drainGcQueue: processes items enqueued before the rebalance, which were correctly
  //    identified as obsolete (offset below globalSafeOffset). Deleting them is safe.
  //  - sweepOrphanedLocks: read-only LIST/GET on revoked partitions; any enqueued GcItems
  //    are discarded by the next drain after open() prunes seekedOffsets.
  @volatile private[seek] var acceptingWork = false

  // Executors are deferred to startExecutors() (called from open()) so that if
  // IndexManagerV2 is constructed but the surrounding WriterManager/task setup
  // fails, no daemon threads leak.
  @volatile private[seek] var executorsStarted = false
  @volatile private[seek] var gcExecutor:       Option[ScheduledExecutorService] = None
  @volatile private[seek] var sweepExecutorOpt: Option[ScheduledExecutorService] = None

  private def startExecutors(): Unit =
    if (!executorsStarted) {
      commitMode match {
        case CommitMode.Granular =>
          // Allocate each executor in two phases: create the pool, then schedule.
          // If scheduleAtFixedRate throws (e.g. RejectedExecutionException, bad interval),
          // we must shut the just-created pool down before rethrowing -- otherwise the
          // outer assignment (gcExecutor = Some(...)) never happens and the daemon thread
          // becomes unreachable and leaks.
          val gcPool = Executors.newSingleThreadScheduledExecutor { (r: Runnable) =>
            val t = new Thread(r, s"gc-${connectorTaskId.show}")
            t.setDaemon(true)
            t
          }
          try {
            gcPool.scheduleAtFixedRate(() => if (acceptingWork) drainGcQueue(),
                                       gcIntervalSeconds.toLong,
                                       gcIntervalSeconds.toLong,
                                       TimeUnit.SECONDS,
            )
          } catch {
            case NonFatal(e) =>
              val _ = gcPool.shutdownNow()
              throw e
          }
          gcExecutor = Some(gcPool)

          if (gcSweepEnabled) {
            val sweepPool = Executors.newSingleThreadScheduledExecutor { (r: Runnable) =>
              val t = new Thread(r, s"sweep-${connectorTaskId.show}")
              t.setDaemon(true)
              t
            }
            try {
              sweepPool.scheduleAtFixedRate(() => if (acceptingWork) sweepOrphanedLocks(),
                                            gcSweepIntervalSeconds.toLong,
                                            gcSweepIntervalSeconds.toLong,
                                            TimeUnit.SECONDS,
              )
            } catch {
              case NonFatal(e) =>
                val _ = sweepPool.shutdownNow()
                gcExecutor.foreach(_.shutdownNow())
                gcExecutor = None
                throw e
            }
            sweepExecutorOpt = Some(sweepPool)
          }

        case CommitMode.Batch =>
          // Batch mode never creates granular lock files, so the granular GC drain has nothing
          // to collect -- `gcExecutor` stays None. The `.temp-upload` orphan sweep always runs
          // inline at the end of every `open()` (age-gated, connector/topic/partition-scoped),
          // unconditionally -- that one-off, ownership-change-triggered sweep is cheap and does
          // not gate on `gcSweepEnabled`. A periodic companion, gated by `gcSweepEnabled` like
          // the granular sweep it mirrors, additionally reaps orphans for a long-lived task that
          // sees no further rebalances (so no further `open()` calls) between staging failures /
          // CAS losses.
          if (gcSweepEnabled) {
            val sweepPool = Executors.newSingleThreadScheduledExecutor { (r: Runnable) =>
              val t = new Thread(r, s"batch-temp-sweep-${connectorTaskId.show}")
              t.setDaemon(true)
              t
            }
            try {
              sweepPool.scheduleAtFixedRate(() => if (acceptingWork) sweepAllBatchTemps(),
                                            gcSweepIntervalSeconds.toLong,
                                            gcSweepIntervalSeconds.toLong,
                                            TimeUnit.SECONDS,
              )
            } catch {
              case NonFatal(e) =>
                val _ = sweepPool.shutdownNow()
                throw e
            }
            sweepExecutorOpt = Some(sweepPool)
          }
      }

      executorsStarted = true
    }

  /**
   * Periodic (only when `gcSweepEnabled`) companion to the at-`open()` `.temp-upload` sweep:
   * reaps orphans for every TP this task currently owns without requiring another
   * `open()`/rebalance. Best-effort per TP; one TP's failure does not block the others.
   */
  private[seek] def sweepAllBatchTemps(): Unit =
    for (tp <- seekedOffsets.keys.toList) {
      bucketAndPrefixFn(tp) match {
        case Right(loc) => sweepBatchTemps(tp, loc)
        case Left(err) =>
          logger.warn(
            s"[${connectorTaskId.show}] Scheduled batch temp sweep: could not resolve bucket/prefix for " +
              s"$tp: ${err.message()}",
          )
      }
    }

  /**
   * Opens a set of topic partitions for writing. If an index file is not found,
   * a new one is created.
   *
   * @param topicPartitions A set of `TopicPartition` objects to open.
   * @return An `Either` containing a `SinkError` on failure or a map of
   *         `TopicPartition` to `Option[Offset]` on success.
   */
  override def open(topicPartitions: Set[TopicPartition]): Either[SinkError, Map[TopicPartition, Option[Offset]]] = {
    startExecutors()

    // Prune state for partitions that were in the previous assignment but are not in the
    // new one (revoked during a rebalance). During shutdown close() is followed by stop(),
    // not open(), so this branch is never reached on the shutdown path -- seekedOffsets
    // remain populated for the final drainGcQueue() in IndexManagerV2.close().
    val stalePartitions = seekedOffsets.keys.toSet -- topicPartitions
    if (stalePartitions.nonEmpty) {
      logger.info(
        s"[${connectorTaskId.show}] Clearing stale state for ${stalePartitions.size} revoked partition(s) " +
          s"from previous assignment: ${stalePartitions.mkString(", ")}",
      )
      stalePartitions.foreach { tp =>
        evictAllGranularLocks(tp)
        clearTopicPartitionState(tp)
      }
    }

    val result = topicPartitions.toList
      .parTraverse(tp => EitherT(IO(open(tp))).map(tp -> _))
      .map(_.toMap)
      .value
      .unsafeRunSync()

    result match {
      case r @ Right(_) =>
        acceptingWork = true
        r
      case l @ Left(_) =>
        // Per-partition open() mutates seekedOffsets / topicPartitionToETags /
        // granularCache as each fiber completes. parTraverse propagates the first
        // Left, but successful fibers have already committed their in-memory state.
        // Roll back every partition in the requested set so the index manager is
        // left in a clean pre-open state and a subsequent open() starts fresh.
        topicPartitions.foreach { tp =>
          evictAllGranularLocks(tp)
          clearTopicPartitionState(tp)
        }
        l
    }
  }

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

    val path = generateLockFilePath(connectorTaskId, topicPartition, directoryFileName)
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      offset <- decideOpen(
        topicPartition,
        path,
        bucketAndPrefix,
        tryOpen(bucketAndPrefix.bucket, path),
        MaxOpenCreateRaceAttempts,
      )
      // Batch mode only, strictly after master resolution and the ownership bump: fence every
      // legacy granular lock left by a granular-mode deployment and purge them once resolved.
      // Entirely best-effort -- see `snapshotLegacyLocks` -- it never fails `open()`.
      _ = if (commitMode == CommitMode.Batch) snapshotLegacyLocks(topicPartition, bucketAndPrefix)
      // Batch mode only, strictly after legacy resolution: reap orphaned batch temp objects.
      // Best-effort — never fails open.
      _ = if (commitMode == CommitMode.Batch) sweepBatchTemps(topicPartition, bucketAndPrefix)
    } yield offset

  }

  /**
   * Deletes orphaned batch temp objects under this task's connector-scoped prefix
   * `.temp-upload/<connector>/<topic>/<partition>/`. An orphan is any object older than
   * `gcSweepMinAgeSeconds` that is not the source of a `CopyOperation` still referenced by the
   * master lock's `PendingState`. That exclusion is merely defensive on the at-`open()` call site
   * (a resolved open leaves `pendingState = None`) but load-bearing on the periodic
   * `sweepAllBatchTemps` call site, which runs concurrently with live commits — hence the
   * fail-closed handling of an unreadable lock below. Never touches paths outside the prefix —
   * granular temps live under `.temp-upload/<topic>/<partition>/` and other connectors under
   * their own name. Failures are logged, never fatal.
   */
  private def sweepBatchTemps(topicPartition: TopicPartition, bucketAndPrefix: CloudLocation): Unit =
    try {
      val prefix       = s".temp-upload/${connectorTaskId.name}/${topicPartition.topic}/${topicPartition.partition}/"
      val ageThreshold = Instant.now().minusSeconds(gcSweepMinAgeSeconds.toLong)
      // Exclusion: any Copy source still recorded in the master lock's PendingState. On the
      // at-`open()` path this is purely defensive (a resolved open leaves pendingState = None),
      // but on the periodic `sweepAllBatchTemps` path it is the ONLY thing keeping a live
      // commit's in-flight Copy sources out of the orphan set, so an unreadable lock must NOT
      // be read as "nothing is referenced". Fail closed and skip the cycle, exactly as
      // `isSweepDueForPartition` does for the sweep marker; the next sweep retries.
      val masterPath = generateLockFilePath(connectorTaskId, topicPartition, directoryFileName)
      val referencedOrSkip: Either[FileLoadError, Set[String]] =
        tryOpen(bucketAndPrefix.bucket, masterPath) match {
          case Right(master) =>
            master.wrappedObject.pendingState.toList
              .flatMap(_.pendingOperations.toList)
              .collect { case c: CopyOperation => c.source }
              .toSet
              .asRight
          // No lock, or a 0-byte lock that cannot carry a PendingState by construction
          // (see the EmptyFileError arm of `decideOpen`): nothing can be referenced.
          case Left(_: FileNotFoundError) => Set.empty[String].asRight
          case Left(_: EmptyFileError) => Set.empty[String].asRight
          case Left(err) => err.asLeft
        }
      referencedOrSkip match {
        case Left(err) =>
          logger.warn(
            s"[${connectorTaskId.show}] Batch temp sweep skipped for $topicPartition: master lock unreadable, " +
              s"cannot tell in-flight Copy sources from orphans: ${err.message()}",
          )
        case Right(referenced) => sweepOrphansUnder(topicPartition, bucketAndPrefix, prefix, ageThreshold, referenced)
      }
    } catch {
      case NonFatal(e) =>
        logger.warn(s"[${connectorTaskId.show}] Batch temp sweep threw for $topicPartition; ignoring", e)
    }

  /**
   * LIST-and-delete half of [[sweepBatchTemps]]: deletes every object under `prefix` older than
   * `ageThreshold` that is not in `referenced`.
   */
  private def sweepOrphansUnder(
    topicPartition:  TopicPartition,
    bucketAndPrefix: CloudLocation,
    prefix:          String,
    ageThreshold:    Instant,
    referenced:      Set[String],
  ): Unit =
    storageInterface.listFileMetaRecursive(bucketAndPrefix.bucket, Some(prefix)) match {
      case Right(maybeListing) =>
        val orphans = maybeListing.toList
          .flatMap(_.files)
          .collect { case fm: FileMetadata => fm }
          .filter(fm =>
            fm.file.startsWith(prefix) && !fm.lastModified.isAfter(ageThreshold) && !referenced.contains(fm.file),
          )
          .map(_.file)
        if (orphans.nonEmpty) {
          // Chunked: a single `deleteFiles` call with every orphan can exceed a provider's
          // batch-delete limit (S3 rejects >1000 keys), which would otherwise fail permanently
          // for a high-cardinality partition. Each chunk is independent and idempotent, so a
          // partial failure just leaves the failed chunk's temps for the next sweep.
          var deleted = 0
          var failed  = 0
          orphans.grouped(gcBatchSize).foreach { chunk =>
            storageInterface.deleteFiles(bucketAndPrefix.bucket, chunk) match {
              case Left(err) =>
                failed += chunk.size
                logger.warn(
                  s"[${connectorTaskId.show}] Batch temp sweep failed to delete ${chunk.size} orphan(s) for " +
                    s"$topicPartition: ${err.message()}",
                )
              case Right(_) =>
                deleted += chunk.size
            }
          }
          if (deleted > 0) {
            logger.debug(
              s"[${connectorTaskId.show}] Batch temp sweep deleted $deleted orphan(s) for $topicPartition" +
                (if (failed > 0) s" ($failed failed and will be retried on the next sweep)" else ""),
            )
          }
        }
      case Left(err) =>
        logger.warn(
          s"[${connectorTaskId.show}] Batch temp sweep LIST failed for $topicPartition: ${err.message()}",
        )
    }

  /**
   * Decides how to open a master lock given the result of reading it, and is re-entrant so a
   * lost NoOverwrite create race can re-read the now-existing lock and route it through the
   * SAME arms (crucially, a re-read lock carrying a PendingState flows through
   * `processPendingOperations`, not a naive eTag adoption — that is what makes recovery
   * correct, see the architecture note on the reverted create-race adoption fix).
   *
   * @param tryOpenResult The result of `tryOpen` for this lock path.
   * @param attemptsLeft  Bounded re-read budget for the lost-create-race cycle.
   */
  private def decideOpen(
    topicPartition:  TopicPartition,
    path:            String,
    bucketAndPrefix: CloudLocation,
    tryOpenResult:   Either[FileLoadError, ObjectWithETag[IndexFile]],
    attemptsLeft:    Int,
  ): Either[SinkError, Option[Offset]] =
    tryOpenResult match {

      case Left(FileNotFoundError(_, _)) =>
        createNewIndexFileNoOverwrite(topicPartition, path, bucketAndPrefix) match {
          case Right(written) =>
            updateDataReturnOffset(topicPartition, written).asRight[SinkError]

          case Left(_: LostCreateRaceError) if attemptsLeft > 1 =>
            // Another task won the create. The lock now exists — re-read and re-decide through
            // the same arms so the winner's offset (and any PendingState) is adopted/recovered.
            logger.info(
              s"[${connectorTaskId.show}] Lost master-lock create race for $topicPartition at $path; " +
                s"re-reading existing lock (attempts left: ${attemptsLeft - 1})",
            )
            decideOpen(
              topicPartition,
              path,
              bucketAndPrefix,
              tryOpen(bucketAndPrefix.bucket, path),
              attemptsLeft - 1,
            )

          case Left(_: LostCreateRaceError) =>
            // Bounded retries exhausted: the lock oscillated absent/present. Fail fatally so
            // Connect restarts the task and re-seeks from the durable lock — no worse than the
            // pre-fix behaviour, but only after exhausting the bounded re-reads.
            FatalCloudSinkError(
              s"Exhausted master-lock create-race retries for $topicPartition at $path",
              topicPartition,
            ).asLeft[Option[Offset]]

          case Left(other) => other.asLeft[Option[Offset]]
        }

      case Left(EmptyFileError(_, eTag)) =>
        // The master lock exists as a 0-byte poison blob — residue of Bug B's non-atomic write.
        // A length-0 blob cannot carry committedOffset or PendingState by construction
        // (the serialiser produces at minimum ~40 bytes for any non-null IndexFile).
        // Take ownership via setIfMatch(eTag) instead of setIfNoneMatch("*") — the file
        // exists, so a NoOverwrite write would always 412.
        // NOTE: pendingOperationsProcessors is NOT invoked here — the 0-byte file proves
        // no PendingState was ever durably recorded; the recovery write sets pendingState=None.
        val idx = IndexFile(lockOwner, Option.empty, Option.empty)
        storageInterface.writeBlobToFile(bucketAndPrefix.bucket, path, ObjectWithETag(idx, eTag))
          .leftMap { err: UploadError =>
            new FatalCloudSinkError(err.message(), err.toExceptionOption, topicPartition)
          }
          .map(updateDataReturnOffset(topicPartition, _))

      case Left(fileLoadError: FileLoadError) =>
        new FatalCloudSinkError(fileLoadError.message(), fileLoadError.toExceptionOption, topicPartition).asLeft[
          Option[Offset],
        ]

      case Right(ObjectWithETag(
            IndexFile(_, committedOffset, Some(pendingStateFound @ PendingState(pendingOffset, pendingOperations))),
            eTag,
          )) =>
        // Ownership bump BEFORE driving the chain. Without this, a zombie that CAS'd the
        // PendingState can win the first chain step (its `update` succeeds on the stale eTag)
        // while we are still mid-recovery, so the fence never actually moves to ownership
        // change for this arm -- only the content-preserving owner rewrite does that. Rewriting
        // here (owner only; committedOffset/pendingState unchanged) seats the fence first, then
        // the chain is driven with the NEW eTag so our own first step is the one that wins any
        // race against a lingering zombie.
        storageInterface.writeBlobToFile(
          bucketAndPrefix.bucket,
          path,
          ObjectWithETag(IndexFile(lockOwner, committedOffset, Some(pendingStateFound)), eTag),
        ) match {
          case Left(err) if attemptsLeft > 1 =>
            metrics.incrementMasterLockFailures()
            logger.info(
              s"[${connectorTaskId.show}] Ownership bump lost a race while recovering a pending chain for " +
                s"$topicPartition at $path; re-reading and retrying (attempts left: ${attemptsLeft - 1}): " +
                s"${err.message()}",
            )
            decideOpen(
              topicPartition,
              path,
              bucketAndPrefix,
              tryOpen(bucketAndPrefix.bucket, path),
              attemptsLeft - 1,
            )
          case Left(err) =>
            metrics.incrementMasterLockFailures()
            logger.warn(
              s"[${connectorTaskId.show}] Ownership bump exhausted retries while recovering a pending chain for " +
                s"$topicPartition at $path: ${err.message()}",
            )
            new FatalCloudSinkError(err.message(), err.toExceptionOption, topicPartition).asLeft[Option[Offset]]
          case Right(bumped) =>
            metrics.incrementMasterLockUpdates()
            topicPartitionToETags.put(topicPartition, bumped.eTag)
            pendingOperationsProcessors.processPendingOperations(
              topicPartition,
              committedOffset,
              PendingState(pendingOffset, pendingOperations),
              update,
            ).map { resolvedOffset =>
              // processPendingOperations calls update() which already maintains
              // topicPartitionToETags with the correct eTag. We must NOT overwrite it with the
              // stale bumped eTag. Only ensure seekedOffsets is updated for cases where update()
              // was not called (e.g. when the recovery path cleared a stale PendingState on the
              // last operation).
              resolvedOffset.foreach(o => seekedOffsets.put(topicPartition, o))
              resolvedOffset
            }.leftMap { err =>
              // On failure, the eTag we seeded above may now be stale: an intermediate
              // phase of processPendingOperations may have advanced the index file in
              // storage before the final fnIndexUpdate failed. Drop the cached eTag so
              // the next call re-reads the file and we never issue a conditional write
              // with the wrong If-Match. Mirrors the gcRemove-on-failure pattern used
              // when loading granular locks.
              val _ = topicPartitionToETags.remove(topicPartition)
              err
            }
        }

      case Right(ObjectWithETag(IndexFile(_, committedOffset, None), eTag)) =>
        // Ownership bump. Adopting the eTag without rewriting would leave the previous owner's
        // token valid until this task's first commit, so a zombie could still write the lock in
        // between. Rewriting here moves the fence to ownership change. Content is unchanged apart
        // from the owner field, so nothing a reader depends on moves.
        // A Left means another task raced us for ownership: re-read and re-decide through
        // the same arms (bounded by `attemptsLeft`, shared with the lost-create-race budget)
        // rather than failing `open()` on a single lost race -- the re-read might show the
        // winner's PendingState, which must still be recovered through the arm above, not
        // adopted naively.
        storageInterface.writeBlobToFile(
          bucketAndPrefix.bucket,
          path,
          ObjectWithETag(IndexFile(lockOwner, committedOffset, None), eTag),
        ) match {
          case Right(written) =>
            metrics.incrementMasterLockUpdates()
            updateDataReturnOffset(topicPartition, written).asRight[SinkError]
          case Left(err) if attemptsLeft > 1 =>
            metrics.incrementMasterLockFailures()
            logger.info(
              s"[${connectorTaskId.show}] Master-lock ownership bump lost a race for $topicPartition at $path; " +
                s"re-reading and retrying (attempts left: ${attemptsLeft - 1}): ${err.message()}",
            )
            decideOpen(
              topicPartition,
              path,
              bucketAndPrefix,
              tryOpen(bucketAndPrefix.bucket, path),
              attemptsLeft - 1,
            )
          case Left(err) =>
            metrics.incrementMasterLockFailures()
            logger.warn(
              s"[${connectorTaskId.show}] Master-lock ownership bump exhausted retries for $topicPartition at " +
                s"$path: ${err.message()}",
            )
            new FatalCloudSinkError(err.message(), err.toExceptionOption, topicPartition).asLeft[Option[Offset]]
        }

      case Right(objectWithetag @ ObjectWithETag(IndexFile(_, _, _), _)) =>
        updateDataReturnOffset(topicPartition, objectWithetag).asRight[SinkError]
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
   * Creates a new, empty index file for a topic partition.  Will not overwrite an existing index file.
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
  ): Either[SinkError, ObjectWithETag[IndexFile]] = {
    val idx = IndexFile(lockOwner, Option.empty, Option.empty)
    storageInterface.writeBlobToFile(bucketAndPrefix.bucket, path, NoOverwriteExistingObject(idx))
      .leftMap {
        // Lost the create race (another task created the lock first). Surface a recoverable
        // signal so `decideOpen` re-reads and adopts the winner's lock instead of failing.
        case _: NonOverwriteFileExistsError => LostCreateRaceError(topicPartition): SinkError
        // Any other write failure (permissions, network, disk) is genuinely fatal.
        case err: UploadError =>
          new FatalCloudSinkError(err.message(), err.toExceptionOption, topicPartition): SinkError
      }
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
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      eTag <- topicPartitionToETags.get(topicPartition).toRight {
        FatalCloudSinkError("Index not found", topicPartition)
      }
      index = ObjectWithETag(
        IndexFile(lockOwner, committedOffset, pendingState),
        eTag,
      )
      blobFileWrite <- storageInterface.writeBlobToFile(
        bucketAndPrefix.bucket,
        path,
        index,
      ).leftMap { err: UploadError =>
        new FatalCloudSinkError(err.message(), err.toExceptionOption, topicPartition): SinkError
      }
    } yield updateDataReturnOffset(topicPartition, blobFileWrite)
  }

  override def getSeekedOffsetForTopicPartition(topicPartition: TopicPartition): Option[Offset] =
    seekedOffsets.get(topicPartition)

  /**
   * Batch-mode dedup floor: the master lock's committed offset alone.
   *
   * Deliberately routing-independent: it does NOT consult per-key granular locks, not even
   * during a granular -> batch migration, because a replayed record is not guaranteed to route
   * back to the key that originally handled it -- the very assumption batch mode exists to drop
   * (see the "why batch mode exists" motivation in `docs/datalake-exactly-once-partitionby.md`).
   * Consulting a per-key legacy floor here would silently drop a record that changed keys
   * between the original attempt and the replay, reintroducing that exact bug during the
   * migration window.
   *
   * Trade-off: a granular -> batch migration therefore RE-WRITES (never loses) every record in
   * `(masterAtSwitch, maxLegacyLockAtSwitch]` -- a bounded, one-time duplicate window, accepted
   * as the cost of never dropping a record whose key routing changed. See
   * `docs/datalake-exactly-once-partitionby.md` ("Switching modes") for the full argument and
   * the operational procedure (flush-then-idle before switching) that empties the window.
   *
   * Legacy locks are still fenced and deleted (`snapshotLegacyLocks`) so a granular zombie
   * cannot write a final path using a stale token, but that is now decoupled from
   * deduplication entirely.
   */
  override def batchDedupFloor(topicPartition: TopicPartition): Either[SinkError, Option[Offset]] =
    seekedOffsets.get(topicPartition).asRight

  /**
   * No-op: legacy-lock purge now happens synchronously inside batch-mode `open()`
   * (`snapshotLegacyLocks` / `purgeLegacyLocks`) as soon as every legacy lock on the TP has been
   * fenced -- it no longer waits for a batch watermark to catch up, because `batchDedupFloor`
   * (master-only, see above) never consults these locks. Kept on the trait so
   * `WriterCommitManager.commitBatch` does not need a mode-specific branch to call it.
   */
  override def afterBatchCommit(topicPartition: TopicPartition, committed: Offset): Unit = ()

  /**
   * Fences and reaps every legacy granular lock present on the TP at batch-mode `open()`:
   * resolves any in-flight chain and bumps each clean lock's eTag (fencing a granular zombie
   * that might still be running). Batch-mode dedup no longer consults these locks at all (see
   * `batchDedupFloor`) -- they are fenced/deleted purely so a granular zombie cannot write a
   * final path using a stale token (the one residual this PR documents: a granular zombie
   * creating a brand-new key after this snapshot, mitigated by the stop-first migration
   * procedure).
   *
   * Once every lock on the TP has been resolved, they are deleted immediately alongside the
   * sweep marker -- nothing gates the delete on a batch watermark, because correctness no
   * longer depends on these locks surviving any longer than it takes to fence them.
   *
   * Entirely best-effort and NEVER fails `open()`: a LIST failure, or a single lock that could
   * not be resolved (e.g. it is still racing a live granular zombie), is logged and retried on
   * the next `open()` of this TP (next rebalance/restart). This is safe because the only thing
   * that depends on this succeeding is the residual zombie-writes-a-new-key window above, not
   * the no-loss/no-duplication invariants.
   *
   * Short-circuits via `legacyResolved` once a prior `open()` in this task's lifetime confirmed
   * the TP clear, so a long-lived task with no further rebalances is not stuck re-LISTing the
   * `.locks/` prefix on hypothetical future opens (there are none) -- this guards against
   * repeat work across rebalances within the same JVM, not across restarts.
   */
  private def snapshotLegacyLocks(topicPartition: TopicPartition, bucketAndPrefix: CloudLocation): Unit =
    if (!legacyResolved.getOrElse(topicPartition, false)) {
      try {
        val prefix =
          s"$directoryFileName/${connectorTaskId.name}/.locks/${topicPartition.topic}/${topicPartition.partition}/"
        storageInterface.listFileMetaRecursive(bucketAndPrefix.bucket, Some(prefix)) match {
          case Right(maybeListing) =>
            val lockKeys = maybeListing.toList.flatMap(_.files).collect { case fm: FileMetadata => fm.file }
              .collect {
                case p if p.startsWith(prefix) => p.substring(p.lastIndexOf('/') + 1)
              }.collect {
                // A `*.lock.tmp.<uuid>` orphan does not end in `.lock`; `sweep-marker.json`
                // likewise. Only genuine `<pk>.lock` files are legacy locks.
                case name if name.endsWith(".lock") && TmpOrphanPattern.findFirstIn(name).isEmpty =>
                  name.stripSuffix(".lock")
              }
            if (lockKeys.isEmpty) {
              val _ = legacyResolved.put(topicPartition, true)
            } else {
              val failedKeys = lockKeys.filter { pk =>
                loadLegacy(topicPartition, pk) match {
                  case Right(_) => false
                  case Left(err) =>
                    logger.warn(
                      s"[${connectorTaskId.show}] Failed to fence legacy granular lock " +
                        s"$topicPartition/$pk; will retry on the next open(): ${err.message()}",
                    )
                    true
                }
              }
              if (failedKeys.isEmpty) {
                purgeLegacyLocks(topicPartition, bucketAndPrefix, lockKeys)
              } else {
                logger.warn(
                  s"[${connectorTaskId.show}] ${failedKeys.size} of ${lockKeys.size} legacy lock(s) for " +
                    s"$topicPartition could not be fenced this open(); not purging yet.",
                )
              }
            }
          case Left(err) =>
            logger.warn(
              s"[${connectorTaskId.show}] Failed to list legacy granular locks for $topicPartition: " +
                s"${err.message()}. Will retry on the next open().",
            )
        }
      } catch {
        case NonFatal(e) =>
          logger.warn(s"[${connectorTaskId.show}] Legacy-lock snapshot threw for $topicPartition; ignoring", e)
      }
    }

  /**
   * Deletes every fenced legacy lock plus the (now-unused) sweep marker, chunked by
   * `gcBatchSize` so a provider's batch-delete limit (S3 rejects >1000 keys in one
   * `DeleteObjects` call) cannot permanently block a high-cardinality partition's purge. Each
   * chunk is independent; `legacyResolved` is only set once every chunk succeeds, so a partial
   * failure simply retries the whole set on the next `open()` (re-fencing an already-fenced lock
   * is a harmless no-op bump).
   */
  private def purgeLegacyLocks(
    topicPartition:  TopicPartition,
    bucketAndPrefix: CloudLocation,
    lockKeys:        List[String],
  ): Unit = {
    val lockPaths =
      lockKeys.map(pk => generateGranularLockFilePath(connectorTaskId, topicPartition, pk, directoryFileName))
    val markerPath = generateSweepMarkerPath(connectorTaskId, topicPartition, directoryFileName)
    val allSucceeded = (lockPaths :+ markerPath).grouped(gcBatchSize).map { chunk =>
      storageInterface.deleteFiles(bucketAndPrefix.bucket, chunk) match {
        case Right(_) => true
        case Left(err) =>
          logger.warn(
            s"[${connectorTaskId.show}] Legacy-lock purge for $topicPartition failed on a chunk of " +
              s"${chunk.size}; will retry on the next open(): ${err.message()}",
          )
          false
      }
    }.toList.forall(identity)
    if (allSucceeded) {
      val _ = legacyResolved.put(topicPartition, true)
      gcRemoveAllForTp(topicPartition)
      metrics.incrementLegacyLocksPurged()
      logger.info(
        s"[${connectorTaskId.show}] Purged ${lockPaths.size} legacy granular lock(s) for $topicPartition.",
      )
    }
  }

  /**
   * Loads (fences) one legacy granular lock: resolves an in-flight `PendingState` or bumps a
   * clean lock's eTag, taking ownership of a poison blob if found. On a lost race resolving a
   * pending chain (a live granular zombie also driving it), re-reads and retries up to
   * [[MaxLegacyBumpAttempts]] times before giving up for this cycle -- the caller
   * (`snapshotLegacyLocks`) treats a final failure as non-fatal and retries on the next `open()`.
   */
  private def loadLegacy(topicPartition: TopicPartition, partitionKey: String): Either[SinkError, Option[Offset]] =
    bucketAndPrefixFn(topicPartition).flatMap { bp =>
      val path = generateGranularLockFilePath(connectorTaskId, topicPartition, partitionKey, directoryFileName)

      def attempt(attemptsLeft: Int): Either[SinkError, Option[Offset]] =
        tryOpen(bp.bucket, path) match {
          case Right(owe @ ObjectWithETag(IndexFile(_, committedOffset, Some(pending)), _)) =>
            // Dead-worker recovery: resolve the in-flight chain. The final updateForPartitionKey
            // bumps the eTag, so the lock is fenced too.
            gcPut(topicPartition, partitionKey, GranularCacheEntry(None, owe.eTag))
            pendingOperationsProcessors.processPendingOperations(
              topicPartition,
              committedOffset,
              pending,
              (tp, co, ps) => updateForPartitionKey(tp, partitionKey, co, ps),
            ) match {
              case r @ Right(_) => r
              case Left(_) if attemptsLeft > 1 =>
                logger.info(
                  s"[${connectorTaskId.show}] Lost a race resolving the legacy pending chain for " +
                    s"$topicPartition/$partitionKey; re-reading and retrying (attempts left: " +
                    s"${attemptsLeft - 1})",
                )
                attempt(attemptsLeft - 1)
              case l @ Left(_) => l
            }

          case Right(owe @ ObjectWithETag(IndexFile(_, committedOffset, None), _)) =>
            gcPut(topicPartition, partitionKey, GranularCacheEntry(committedOffset, owe.eTag))
            bumpLegacyLock(topicPartition, partitionKey, committedOffset, MaxLegacyBumpAttempts)

          case Left(_: FileNotFoundError) =>
            Option.empty[Offset].asRight

          case Left(EmptyFileError(_, eTag)) =>
            // Take ownership exactly as ensureGranularLock does.
            storageInterface.writeBlobToFile(
              bp.bucket,
              path,
              ObjectWithETag(IndexFile(lockOwner, Option.empty, Option.empty), eTag),
            ).bimap(
              err => NonFatalCloudSinkError.unswallowable(err.message(), err.toExceptionOption): SinkError,
              owe => {
                gcPut(topicPartition, partitionKey, GranularCacheEntry(None, owe.eTag))
                Option.empty[Offset]
              },
            )

          case Left(err) =>
            NonFatalCloudSinkError.unswallowable(
              s"Failed to load legacy granular lock $topicPartition/$partitionKey: ${err.message()}",
              err.toExceptionOption,
            ).asLeft
        }

      attempt(MaxLegacyBumpAttempts)
    }

  /**
   * Bumps a clean legacy lock's eTag (rewrites `committedOffset` unchanged, `pendingState = None`).
   * On an eTag mismatch — a granular zombie wrote between our read and this bump — re-read and
   * repeat, at most [[MaxLegacyBumpAttempts]] times, then fail fatally. Never uses an offset whose
   * bump did not succeed.
   */
  private def bumpLegacyLock(
    topicPartition:  TopicPartition,
    partitionKey:    String,
    committedOffset: Option[Offset],
    attemptsLeft:    Int,
  ): Either[SinkError, Option[Offset]] =
    updateForPartitionKey(topicPartition, partitionKey, committedOffset, Option.empty) match {
      case Right(_) => committedOffset.asRight
      case Left(_) if attemptsLeft > 1 =>
        bucketAndPrefixFn(topicPartition).flatMap { bp =>
          val path = generateGranularLockFilePath(connectorTaskId, topicPartition, partitionKey, directoryFileName)
          tryOpen(bp.bucket, path) match {
            case Right(owe @ ObjectWithETag(IndexFile(_, co2, None), _)) =>
              gcPut(topicPartition, partitionKey, GranularCacheEntry(co2, owe.eTag))
              bumpLegacyLock(topicPartition, partitionKey, co2, attemptsLeft - 1)
            case Right(owe @ ObjectWithETag(IndexFile(_, co2, Some(pending)), _)) =>
              gcPut(topicPartition, partitionKey, GranularCacheEntry(None, owe.eTag))
              pendingOperationsProcessors.processPendingOperations(
                topicPartition,
                co2,
                pending,
                (tp, co, ps) => updateForPartitionKey(tp, partitionKey, co, ps),
              )
            case Left(rerr) =>
              new FatalCloudSinkError(rerr.message(), rerr.toExceptionOption, topicPartition).asLeft
          }
        }
      case Left(err) =>
        new FatalCloudSinkError(
          s"Exhausted legacy-lock bump attempts for $topicPartition/$partitionKey: ${err.message()}",
          err.exception(),
          topicPartition,
        ).asLeft
    }

  // Cache-first lookup: return the cached offset if present, otherwise fetch the granular lock
  // from cloud storage and populate the cache (lazy load). Returns Right(None) if the lock does
  // not exist in storage yet -- callers must fall back to the master lock offset (via
  // getSeekedOffsetForTopicPartition) in that case to provide a deduplication floor for
  // shouldSkip. See WriterManager.createWriter for the fallback.
  // Returns Left(SinkError) on transient failures so callers can fail-fast rather than
  // silently falling back to a potentially stale master lock offset.
  override def getSeekedOffsetForPartitionKey(
    topicPartition: TopicPartition,
    partitionKey:   String,
  ): Either[SinkError, Option[Offset]] =
    gcGet(topicPartition, partitionKey) match {
      case Some(cached) =>
        metrics.incrementGranularCacheHits()
        cached.offset.asRight
      case None =>
        metrics.incrementGranularCacheMisses()
        loadGranularLock(topicPartition, partitionKey)
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
    // Deterministic path derived from connector task ID, topic-partition, and partition key
    val path = generateGranularLockFilePath(connectorTaskId, topicPartition, partitionKey, directoryFileName)
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      result <- tryOpen(bucketAndPrefix.bucket, path) match {
        // Crash recovery: lock contains a PendingState from an interrupted Upload → Copy → Delete commit
        case Right(objectWithEtag @ ObjectWithETag(
              IndexFile(_, committedOffset, Some(PendingState(pendingOffset, pendingOps))),
              _,
            )) =>
          logger.info(s"Lazy-loading granular lock with PendingState for $topicPartition/$partitionKey")
          // Seed cache with offset=None and the current eTag so processPendingOperations
          // can perform conditional writes; the resolved offset replaces None on success
          gcPut(topicPartition, partitionKey, GranularCacheEntry(None, objectWithEtag.eTag))
          val fnUpdate: (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]] =
            (tp, co, ps) => updateForPartitionKey(tp, partitionKey, co, ps)
          pendingOperationsProcessors.processPendingOperations(
            topicPartition,
            committedOffset,
            PendingState(pendingOffset, pendingOps),
            fnUpdate,
          ).left.map { err =>
            gcRemove(topicPartition, partitionKey)
            err
          }

        // Happy path: no crash recovery needed, cache the resolved offset and eTag directly
        case Right(objectWithEtag @ ObjectWithETag(IndexFile(_, committedOffset, None), _)) =>
          logger.info(s"Lazy-loaded granular lock for $topicPartition/$partitionKey, offset=$committedOffset")
          gcPut(topicPartition, partitionKey, GranularCacheEntry(committedOffset, objectWithEtag.eTag))
          committedOffset.asRight

        // Expected for brand-new partition keys that have never been committed
        case Left(_: FileNotFoundError) =>
          Option.empty[Offset].asRight

        // 0-byte poison blob: residue of Bug B's non-atomic write.
        // Return None offset AND cache the eTag so a subsequent updateForPartitionKey
        // finds the eTag in cache and does not fatal on cache miss (resolveGranularETag
        // would fail with FatalCloudSinkError if there were no cache entry at all).
        // This is the cache-miss load-and-populate pattern — consistent with the existing
        // Right(_) branches — not an exception to the no-re-read rule. The no-re-read rule
        // applies to subsequent reads while a commit is in flight; those are unchanged.
        case Left(EmptyFileError(_, eTag)) =>
          logger.warn(
            s"Granular lock for $topicPartition/$partitionKey is a 0-byte poison blob (eTag=$eTag); caching eTag for recovery overwrite",
          )
          gcPut(topicPartition, partitionKey, GranularCacheEntry(None, eTag))
          Option.empty[Offset].asRight

        // Transient cloud errors (network, throttling): retried by Kafka Connect under RETRY;
        // fail-fast under NOOP/THROW so partially loaded state is never silently swallowed.
        case Left(err) =>
          val sinkError: SinkError = NonFatalCloudSinkError.unswallowable(
            s"Failed to lazy-load granular lock for $topicPartition/$partitionKey: ${err.message()}",
            err.toExceptionOption,
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
  ): Either[SinkError, String] =
    gcGet(topicPartition, partitionKey).map(_.eTag).toRight {
      val error = FatalCloudSinkError(
        s"Granular lock eTag for $topicPartition/$partitionKey not in cache. " +
          s"This may indicate a zombie task whose cache entry was evicted. Failing to preserve fencing.",
        topicPartition,
      )
      logger.error(error.message)
      error
    }

  /**
   * Writes an eTag-conditional update to the granular lock file for a specific partition key.
   *
   * The eTag is resolved from the in-memory cache (never re-read from storage) to preserve
   * the zombie-task fencing invariant. On success the cache is updated with the new offset
   * and eTag so subsequent commits use the fresh fencing token.
   */
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
      // Resolve the cached eTag; fails fatally on miss to preserve zombie-task fencing.
      eTag <- resolveGranularETag(topicPartition, partitionKey)
      index = ObjectWithETag(
        IndexFile(lockOwner, committedOffset, pendingState),
        eTag,
      )
      // eTag-conditional write: succeeds only if the stored eTag matches, preventing zombie overwrites
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
      // Refresh cache with new offset + eTag so the next commit uses the fresh fencing token
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
      // Crash recovery: previous task crashed mid-commit, resolve pending ops before caching
      case ObjectWithETag(IndexFile(_, committedOffset, Some(PendingState(pendingOffset, pendingOps))), _) =>
        logger.info(s"ensureGranularLock found PendingState for $topicPartition/$partitionKey, resolving")
        // Seed cache with offset=None so processPendingOperations can perform conditional writes
        gcPut(topicPartition, partitionKey, GranularCacheEntry(None, objectWithEtag.eTag))
        val fnUpdate: (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]] =
          (tp, co, ps) => updateForPartitionKey(tp, partitionKey, co, ps)
        pendingOperationsProcessors.processPendingOperations(
          topicPartition,
          committedOffset,
          PendingState(pendingOffset, pendingOps),
          fnUpdate,
        ).bimap(
          { err => gcRemove(topicPartition, partitionKey); err },
          _ => (),
        )

      // Clean lock: no crash recovery needed, cache offset and eTag directly
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
    // Fast path: cache hit means the lock file already exists and its eTag is tracked
    if (gcContainsKey(topicPartition, partitionKey)) {
      metrics.incrementGranularCacheHits()
      ().asRight
    } else {
      metrics.incrementGranularCacheMisses()
      val path = generateGranularLockFilePath(connectorTaskId, topicPartition, partitionKey, directoryFileName)
      for {
        bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
        _ <- tryOpen(bucketAndPrefix.bucket, path) match {
          // Lock exists in storage: resolve any PendingState and cache the result
          case Right(objectWithEtag) =>
            resolveAndCacheGranularLock(topicPartition, partitionKey, objectWithEtag)

          // Lock does not exist yet: create an empty one with NoOverwrite precondition
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
              // NoOverwrite failed: another task likely created the file between our read
              // and write (TOCTOU race). Re-read to populate the cache instead.
              logger.info(
                s"NoOverwriteExistingObject write failed for $topicPartition/$partitionKey, " +
                  s"re-reading existing lock (likely created by another task)",
              )
              tryOpen(bucketAndPrefix.bucket, path) match {
                case Right(existing) =>
                  resolveAndCacheGranularLock(topicPartition, partitionKey, existing)
                // Transient cloud error on re-read after NoOverwrite race: retried by Kafka Connect
                // under RETRY; fail-fast under NOOP/THROW to avoid swallowing integrity state.
                case Left(retryErr) =>
                  NonFatalCloudSinkError.unswallowable(retryErr.message(), retryErr.toExceptionOption).asLeft
              }
            }

          // 0-byte poison blob: residue of Bug B's non-atomic write.
          // Use ObjectWithETag(eTag) so the atomic rename uses setIfMatch(eTag) on the
          // DESTINATION slot — taking ownership of the existing poison file.
          // Using NoOverwriteExistingObject here would always 412 (the file exists).
          case Left(EmptyFileError(_, eTag)) =>
            logger.warn(
              s"Granular lock for $topicPartition/$partitionKey is a 0-byte poison blob (eTag=$eTag); " +
                s"overwriting via setIfMatch to recover",
            )
            val idx = IndexFile(lockOwner, None, None)
            storageInterface.writeBlobToFile(
              bucketAndPrefix.bucket,
              path,
              ObjectWithETag(idx, eTag),
            ).map { result =>
              gcPut(topicPartition, partitionKey, GranularCacheEntry(None, result.eTag))
              ()
            }.left.flatMap { _: UploadError =>
              // 412 on eTag-conditional overwrite: another task won the race and wrote real
              // content into the previously-empty slot. Re-read to populate the cache.
              tryOpen(bucketAndPrefix.bucket, path) match {
                case Right(existing) =>
                  resolveAndCacheGranularLock(topicPartition, partitionKey, existing)
                case Left(retryErr) =>
                  NonFatalCloudSinkError.unswallowable(retryErr.message(), retryErr.toExceptionOption).asLeft
              }
            }

          // Transient storage error: retried by Kafka Connect under RETRY; fail-fast under NOOP/THROW.
          case Left(err) =>
            NonFatalCloudSinkError.unswallowable(err.message(), err.toExceptionOption).asLeft
        }
      } yield ()
    }

  /** Removes a single partition key from the granular cache and updates the metrics gauge. */
  override def evictGranularLock(
    topicPartition: TopicPartition,
    partitionKey:   String,
  ): Unit = {
    gcRemove(topicPartition, partitionKey)
    metrics.setGranularCacheSize(granularCacheSize)
  }

  /** Removes all granular cache entries for the given topic-partition and updates the metrics gauge. */
  override def evictAllGranularLocks(
    topicPartition: TopicPartition,
  ): Unit = {
    gcRemoveAllForTp(topicPartition)
    metrics.setGranularCacheSize(granularCacheSize)
  }

  /** Removes seeked offset and master-lock eTag for this partition, preventing background threads from operating on stale state. */
  override def clearTopicPartitionState(topicPartition: TopicPartition): Unit = {
    seekedOffsets.remove(topicPartition)
    val _ = topicPartitionToETags.remove(topicPartition)
    // A revoked/failed-open partition must re-confirm legacy-lock resolution on its next open()
    // rather than trusting a flag set under the previous (possibly abandoned) ownership episode.
    val _ = legacyResolved.remove(topicPartition)
  }

  /**
   * Writes the master lock with `globalSafeOffset - 1` as the committed offset, or `None`
   * when `globalSafeOffset == 0` (nothing durably committed yet).
   *
   * The minus-one conversion preserves the existing semantic that `committedOffset` is the
   * highest offset durably in storage, ensuring backward compatibility with older code that
   * may read this lock file. When `globalSafeOffset == 0`, `None` avoids the false claim
   * that offset 0 is committed and prevents HWM inflation on restart. The eTag is
   * deliberately NOT refreshed on write failure -- this preserves fencing against zombie
   * tasks (see architecture doc).
   */
  // TEST-ONLY SEAM — DO NOT WIRE FROM PRODUCTION CODE.
  // No-op by default. Fires inside `updateMasterLock`, between the cached-eTag read and the
  // eTag-conditional storage write, to engineer a deterministic zombie rebalance race in tests.
  // See `IndexManagerV2TestHooks` for the install/clear API.
  @volatile private[seek] var testHookPreWriteMasterLockBarrier: () => Unit = () => ()

  override def updateMasterLock(
    topicPartition:   TopicPartition,
    globalSafeOffset: Offset,
  ): Either[SinkError, Unit] = {
    val path = generateLockFilePath(connectorTaskId, topicPartition, directoryFileName)
    // Store globalSafeOffset - 1 to preserve the "highest committed offset" semantic.
    // When globalSafeOffset == 0, nothing has been durably committed, so persist None
    // rather than the misleading Some(Offset(0)). None means open() will not call
    // context.offset(), and the consumer will use the consumer group position (which
    // preCommit keeps at 0 while globalSafeOffset == 0).
    val committedOffset =
      if (globalSafeOffset.value == 0L) None
      else Some(Offset(globalSafeOffset.value - 1L))
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      eTag <- topicPartitionToETags.get(topicPartition).toRight {
        FatalCloudSinkError("Master index not found", topicPartition)
      }
      index = ObjectWithETag(
        IndexFile(lockOwner, committedOffset, None),
        eTag,
      )
      _ = testHookPreWriteMasterLockBarrier() // test seam — no-op in production
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
      // Update cached eTag and seeked offset so subsequent writes use the fresh fencing token
      topicPartitionToETags.put(topicPartition, blobFileWrite.eTag)
      blobFileWrite.wrappedObject.committedOffset.foreach(o => seekedOffsets.put(topicPartition, o))
      ()
    }
  }

  /**
   * Synchronous GC enqueue phase: scans the granular cache for this partition, identifies
   * entries whose committed offset is strictly below `globalSafeOffset - 1` (i.e., below
   * `masterOffset`) and that are NOT protected by an active writer, evicts them from the
   * cache, and enqueues their cloud paths into `gcQueue` for asynchronous deletion by
   * `drainGcQueue`. Performs no cloud I/O itself.
   *
   * The threshold preserves the granular lock at `masterOffset` (`globalSafeOffset - 1`).
   * On restart, `context.offset(tp, masterOffset)` replays that record, and the granular
   * lock is needed by `shouldSkip` to deduplicate it (the one-record-overlap invariant).
   * The threshold matches `readAndEnqueue` in `sweepOrphanedLocks`.
   */
  override def cleanUpObsoleteLocks(
    topicPartition:      TopicPartition,
    globalSafeOffset:    Offset,
    activePartitionKeys: Set[String],
  ): Either[SinkError, Unit] =
    Option(granularCache.get(topicPartition)).filterNot(_.isEmpty) match {
      case None        => ().asRight
      case Some(inner) =>
        // Collect partition keys eligible for GC: (offset below threshold OR offset unset)
        // AND no active writer. The offset.isEmpty branch handles cached entries seeded by
        // ensureGranularLock that never received a commit; without it those empty locks
        // would never be enqueued and would accumulate in cloud storage. Active writers
        // are still protected by the activePartitionKeys check.
        val keysToRemove = inner.entrySet().asScala.collect {
          case entry
              if (entry.getValue.offset.isEmpty ||
                entry.getValue.offset.exists(_.value < globalSafeOffset.value - 1L)) &&
                !activePartitionKeys.contains(entry.getKey) =>
            entry.getKey
        }.toList

        if (keysToRemove.isEmpty) ().asRight
        else
          bucketAndPrefixFn(topicPartition).map { bucketAndPrefix =>
            // Evict from cache and enqueue for async deletion; no cloud I/O here
            keysToRemove.foreach { pk =>
              gcRemove(topicPartition, pk)
              val path = generateGranularLockFilePath(connectorTaskId, topicPartition, pk, directoryFileName)
              gcQueue.add(GcItem(bucketAndPrefix.bucket, path, topicPartition, pk))
            }
            metrics.incrementGcLocksEnqueued(keysToRemove.size.toLong)
            metrics.setGranularCacheSize(granularCacheSize)
            val depth = gcQueue.size()
            metrics.setGcQueueDepth(depth)
            if (depth > 10000) {
              logger.warn(
                s"GC queue depth is $depth for $topicPartition. " +
                  s"Consider reducing connect.<prefix>.indexes.gc.interval.seconds " +
                  s"(currently ${gcIntervalSeconds}s) to drain faster.",
              )
            }
            logger.debug(
              s"Enqueued ${keysToRemove.size} obsolete granular lock(s) for async deletion for $topicPartition",
            )
          }
    }

  /**
   * Asynchronous GC drain phase. Runs on a background timer and also as a final
   * synchronous flush during `close()`.
   *
   * Three-phase logic:
   *  1. Poll all items from `gcQueue`, filtering out revoked partitions (no longer in
   *     `seekedOffsets`) and reclaimed keys (back in `granularCache` due to a new writer).
   *  2. Group eligible items by bucket and chunk into batches of `gcBatchSize`.
   *  3. Delete each batch via `storageInterface.deleteFiles`; on failure, re-enqueue items
   *     up to `MaxGcRetries` times. Failures are logged but never propagated.
   */
  private[seek] def drainGcQueue(): Unit = {
    // Phase 1: drain queue, applying partition-revoked and key-reclaimed filters
    val eligible = Iterator.continually(gcQueue.poll()).takeWhile(_ != null).filter { item =>
      if (!seekedOffsets.contains(item.topicPartition)) {
        logger.debug(
          s"GC discarding ${item.topicPartition}/${item.partitionKey}: partition no longer owned by this task",
        )
        metrics.incrementGcLocksSkippedRevoked()
        false
      } else if (item.kind == GcKind.Lock && gcContainsKey(item.topicPartition, item.partitionKey)) {
        // Only `.lock` items honour the cache-reclaim filter: a populated
        // granularCache entry means a new writer has taken the live `.lock`
        // path, so deleting it would clobber the active lock. `.tmp` orphans
        // (GcKind.TmpOrphan) bypass this branch -- their path is distinct
        // from the active `.lock`, and they are stale by definition.
        logger.debug(s"GC skipping ${item.topicPartition}/${item.partitionKey}: reclaimed by new writer")
        metrics.incrementGcLocksSkippedReclaimed()
        false
      } else {
        true
      }
    }.toList
    metrics.setGcQueueDepth(gcQueue.size())

    // `unprocessed` tracks items that have been polled out of gcQueue but not yet
    // either deleted (Right) or explicitly handled as a Left (which has its own
    // retry re-enqueue path). On an unexpected NonFatal throw anywhere below, we
    // re-offer everything still in this set so no polled work is silently lost.
    val unprocessed = scala.collection.mutable.Set.from(eligible)

    try {
      if (eligible.nonEmpty) {
        // Phase 2: group by bucket for batched deletes
        val byBucket: Map[String, Seq[GcItem]] = eligible.groupBy(_.bucket)

        // Phase 3: batched delete with retry on failure
        byBucket.foreach {
          case (bucket, items) =>
            items.grouped(gcBatchSize).foreach { chunk =>
              storageInterface.deleteFiles(bucket, chunk.map(_.path)) match {
                case Left(err) =>
                  metrics.incrementGcDeleteFailures()
                  logger.warn(
                    s"Background GC batch delete failed for ${chunk.size} lock file(s) from bucket=$bucket: ${err.message()}. " +
                      s"Re-enqueuing conservatively (some items may have been deleted -- idempotent retry is safe).",
                  )
                  val retryable = chunk.filter(_.retryCount < MaxGcRetries)
                  retryable.foreach(i => gcQueue.add(i.copy(retryCount = i.retryCount + 1)))
                  if (retryable.nonEmpty) {
                    metrics.incrementGcDeleteRetries(retryable.size.toLong)
                    logger.debug(
                      s"Re-enqueued ${retryable.size} item(s) for retry (dropped ${chunk.size - retryable.size} that exceeded max retries)",
                    )
                  }
                  unprocessed --= chunk
                case Right(_) =>
                  metrics.incrementGcLocksDeleted(chunk.size.toLong)
                  logger.debug(s"Background GC deleted ${chunk.size} lock file(s) from bucket=$bucket")
                  unprocessed --= chunk
              }
            }
        }
      }
    } catch {
      // InterruptedException is deliberately handled BEFORE NonFatal because
      // `scala.util.control.NonFatal` excludes InterruptedException by design. Without an
      // explicit case here the signal would propagate uncaught, the polled items would not
      // be re-offered, and the executor's interrupt flag would be cleared by the time the
      // exception bubbled up — silently dropping the cancellation request.
      case _: InterruptedException =>
        unprocessed.foreach(gcQueue.offer)
        if (unprocessed.nonEmpty) {
          logger.warn(
            s"Re-enqueued ${unprocessed.size} GC item(s) after interruption for ${connectorTaskId.show}",
          )
        }
        // Restore interrupt status so cooperative shutdown (close() awaitTermination) still
        // observes the cancellation request.
        Thread.currentThread().interrupt()
        logger.info(s"Background GC drain interrupted for ${connectorTaskId.show}; exiting drain loop")

      case NonFatal(e) =>
        // Re-offer any items we polled but could not send through the Right/Left
        // branches -- otherwise an unexpected throw (bug, metric failure, etc.)
        // silently deletes them from gcQueue and leaves their cloud-side lock
        // files orphaned until the next sweep.
        unprocessed.foreach(gcQueue.offer)
        if (unprocessed.nonEmpty) {
          logger.warn(
            s"Re-enqueued ${unprocessed.size} GC item(s) after unexpected drain error for ${connectorTaskId.show}",
          )
        }
        logger.warn(s"Unexpected error in background GC drain for ${connectorTaskId.show}", e)
    }
  }

  /**
   * Periodic orphan sweep: discovers granular lock files in cloud storage that are not
   * tracked by the in-memory cache (leftover from prior task instances or evicted keys)
   * and enqueues them into `gcQueue` for deletion by `drainGcQueue`.
   *
   * Partitions are processed in random order for fairness when the GET budget is limited.
   * Each partition is gated by a persistent sweep marker so that only one sweep runs per
   * configured interval, even across restarts and rebalances.
   */
  private[seek] def sweepOrphanedLocks(): Unit =
    if (gcSweepEnabled) {
      try {
        metrics.incrementSweepRuns()
        // Global GET budget shared across all partitions in this sweep cycle
        var readsRemaining = gcSweepMaxReads
        var totalEnqueued  = 0
        var tpsScanned     = 0
        var tpsSkipped     = 0
        val ageThreshold   = Instant.now().minusSeconds(gcSweepMinAgeSeconds.toLong)
        val now            = System.currentTimeMillis()

        // Randomize partition order so no single partition monopolizes the GET budget.
        //
        // Note: when `globalSafeOffset == 0`, `updateMasterLock` stores a master lock
        // with `committedOffset = None` and does NOT populate `seekedOffsets` for this
        // TP. That case is handled by *absence* here -- the TP is not in
        // `seekedOffsets.keys`, so it is skipped for this sweep cycle. This is correct:
        // no record has been durably committed yet, so there are no granular locks
        // that could be eligible for GC below `masterOffset`. The `.foreach` on
        // `seekedOffsets.get(tp)` is therefore a no-op when `globalSafeOffset == 0`.
        for (tp <- Random.shuffle(seekedOffsets.keys.toList) if readsRemaining > 0) {
          seekedOffsets.get(tp).foreach { masterOffset =>
            bucketAndPrefixFn(tp) match {
              case Right(loc) =>
                val bucket = loc.bucket
                isSweepDueForPartition(bucket, tp, now) match {
                  case Some(protection) =>
                    // Write-before-sweep fencing: the eTag-conditional marker write acts as a
                    // distributed lock. Only the task that wins the conditional write proceeds
                    // with the expensive LIST+GET scan; losers skip this cycle entirely.
                    if (writeSweepMarkerForPartition(bucket, tp, protection)) {
                      tpsScanned += 1
                      val (enqueued, readsUsed) = sweepPartition(tp, masterOffset, ageThreshold, readsRemaining)
                      totalEnqueued += enqueued
                      readsRemaining -= readsUsed
                    } else {
                      tpsSkipped += 1
                    }
                  case None =>
                    tpsSkipped += 1
                }
              case Left(err) =>
                // Misconfigured bucketAndPrefixFn used to be a silent skip. Log so
                // operators can diagnose why a partition is never sweeping.
                logger.warn(
                  s"Sweep: could not resolve bucket/prefix for $tp, skipping this cycle: ${err.message()}",
                )
                tpsSkipped += 1
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
    }

  /**
   * Checks whether the sweep marker for a specific TopicPartition has expired.
   * Returns `Some(protection)` carrying the eTag-based write precondition when the sweep is due,
   * or `None` when the marker is still valid (not yet expired) or unreadable.
   */
  private def isSweepDueForPartition(
    bucket: String,
    tp:     TopicPartition,
    now:    Long,
  ): Option[ObjectProtection[SweepMarker]] = {
    val markerPath = generateSweepMarkerPath(connectorTaskId, tp, directoryFileName)
    val newMarker  = SweepMarker(now, now + gcSweepIntervalSeconds * 1000L)
    storageInterface.getBlobAsObject[SweepMarker](bucket, markerPath) match {
      case Right(ObjectWithETag(marker, _)) if marker.nextRunEpochMillis > now => None
      case Right(ObjectWithETag(_, eTag))                                      => Some(ObjectWithETag(newMarker, eTag))
      case Left(_: FileNotFoundError) => Some(NoOverwriteExistingObject(newMarker))
      case Left(err) =>
        logger.warn(s"Transient error reading sweep marker for $tp in bucket=$bucket, skipping: ${err.message()}")
        None
    }
  }

  /**
   * Writes a sweep marker for a specific TopicPartition using an eTag-conditional write.
   * Returns `true` if the write succeeded (this task won the race), `false` otherwise.
   */
  private def writeSweepMarkerForPartition(
    bucket:     String,
    tp:         TopicPartition,
    protection: ObjectProtection[SweepMarker],
  ): Boolean = {
    val markerPath = generateSweepMarkerPath(connectorTaskId, tp, directoryFileName)
    import IndexManagerV2.SweepMarker.sweepMarkerEncoder
    storageInterface.writeBlobToFile(bucket, markerPath, protection) match {
      case Left(err) =>
        logger.info(s"Sweep marker write lost race for $tp in bucket=$bucket (another task won): $err")
        false
      case Right(_) => true
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
    val result = for {
      bucket <- bucketAndPrefixFn(tp) match {
        case Right(loc) => Some(loc.bucket)
        case Left(err)  =>
          // sweepOrphanedLocks already logged the Left before reaching here, but we
          // are also callable directly from tests and future call sites. Keep a
          // diagnostic line rather than a silent None.
          logger.warn(s"Sweep: could not resolve bucket/prefix for $tp: ${err.message()}")
          None
      }
      listing <- storageInterface.listFileMetaRecursive(bucket, Some(prefix)) match {
        case Left(err) =>
          logger.warn(s"Sweep: failed to list files for $tp: ${err.message()}")
          None
        case Right(maybeListing) => maybeListing
      }
    } yield {
      val files = listing.files.collect { case fm: FileMetadata => fm }
      // Sweep orphaned .lock.tmp.<uuid> blobs: GET-free, uses listing metadata only.
      // The ageThreshold must remain generous (default 86400s) to exceed the worst-case
      // rename round-trip + retry budget. Do NOT tighten without re-analysing rename latency.
      //
      // The match is anchored at the END of the basename and requires a UUID-shaped suffix
      // ([0-9a-fA-F-]+) after `.lock.tmp.`. A naive `contains(".lock.tmp.")` would
      // misclassify legitimate granular locks whose partitionKey itself contains
      // `.lock.tmp.` (partition values are URL-encoded by `WriterManager.sanitize`, which
      // preserves the literal `.` character) -- e.g. `name=report.lock.tmp.archive.lock`
      // would otherwise be enqueued for deletion under GcKind.TmpOrphan, which bypasses
      // the cache-reclaim filter in drainGcQueue.
      files.foreach { fileMeta =>
        val p = fileMeta.file
        if (p.startsWith(prefix) && !fileMeta.lastModified.isAfter(ageThreshold)) {
          val fileName = p.substring(p.lastIndexOf('/') + 1)
          fileName match {
            case TmpOrphanPattern(partitionKey) =>
              logger.debug(s"Sweep: enqueuing orphaned .tmp blob $p for deletion")
              // GcKind.TmpOrphan: bypass the cache-reclaim filter in drainGcQueue.
              // A populated granularCache entry for `partitionKey` belongs to the
              // active `.lock`, never to this stale `.tmp`. See GcKind comment.
              gcQueue.add(GcItem(bucket, p, tp, partitionKey, GcKind.TmpOrphan))
            case _ => ()
          }
        }
      }
      // Classify each .lock file and GET-read only those that pass the filter chain, respecting the budget
      files.foldLeft((0, 0)) {
        case (acc @ (_, readsUsed), _) if readsUsed >= readsRemaining => acc
        case ((enqueued, readsUsed), fileMeta) =>
          classifyLockFile(tp, fileMeta, ageThreshold, prefix) match {
            case SweepSkip                         => (enqueued, readsUsed)
            case SweepDeleteDirectly(partitionKey) =>
              // size==0 from listing metadata: enqueue without GET (ADLS-only optimisation)
              gcQueue.add(GcItem(bucket, fileMeta.file, tp, partitionKey))
              (enqueued + 1, readsUsed)
            case SweepNeedsRead(partitionKey) =>
              val didEnqueue = readAndEnqueue(bucket, fileMeta.file, tp, partitionKey, masterOffset)
              (enqueued + (if (didEnqueue) 1 else 0), readsUsed + 1)
          }
      }
    }
    result.getOrElse((0, 0))
  }

  private sealed trait SweepClassification
  private case object SweepSkip extends SweepClassification
  private case class SweepNeedsRead(partitionKey: String) extends SweepClassification
  // ADLS-only: listing metadata proves size==0; enqueue for deletion without a GET.
  private case class SweepDeleteDirectly(partitionKey: String) extends SweepClassification

  /** Applies prefix-membership, extension, recency, and cache-presence filters to decide whether a lock file needs a GET read. */
  private def classifyLockFile(
    tp:           TopicPartition,
    fileMeta:     FileMetadata,
    ageThreshold: Instant,
    prefix:       String,
  ): SweepClassification = {
    val path = fileMeta.file
    if (!path.startsWith(prefix) || !path.endsWith(".lock") || fileMeta.lastModified.isAfter(ageThreshold)) SweepSkip
    else {
      val fileName     = path.substring(path.lastIndexOf('/') + 1)
      val partitionKey = fileName.stripSuffix(".lock")
      if (gcContainsKey(tp, partitionKey)) SweepSkip
      // ADLS-only optimisation: if the listing metadata proves size==0 and the file is past
      // the ageThreshold, enqueue for deletion without a GET. Prevents indefinite accumulation
      // of 0-byte poison blobs from Bug B's non-atomic write. S3/GCP return size=None and
      // fall through to the normal GET-based classification below.
      else if (fileMeta.size.contains(0L)) SweepDeleteDirectly(partitionKey)
      else SweepNeedsRead(partitionKey)
    }
  }

  /**
   * GETs the lock file and enqueues it for deletion if its committedOffset is strictly below
   * the master lock's committedOffset. The master lock stores `masterOffset = globalSafeOffset - 1`,
   * so `committedOffset < masterOffset` is equivalent to `committedOffset < globalSafeOffset - 1`,
   * matching the threshold used by `cleanUpObsoleteLocks`.
   *
   * The strict-less-than preserves the granular lock at `masterOffset` itself. On restart,
   * `context.offset(tp, masterOffset)` replays that record, and the granular lock is needed
   * by `shouldSkip` to deduplicate it (the one-record-overlap invariant).
   *
   * Lock files with PendingState are also safe to sweep when their committedOffset is
   * below the master: the master lock can only advance past an offset once all writers have
   * committed it.
   */
  private def readAndEnqueue(
    bucket:       String,
    path:         String,
    tp:           TopicPartition,
    partitionKey: String,
    masterOffset: Offset,
  ): Boolean =
    storageInterface.getBlobAsObject[IndexFile](bucket, path) match {
      case Left(_: EmptyFileError) =>
        // 0-byte blob: classifyLockFile has already enforced the ageThreshold filter,
        // so this file is provably older than the worst-case create-flush window
        // and cannot be a live writer's in-progress .tmp. Safe to delete without GET content.
        gcQueue.add(GcItem(bucket, path, tp, partitionKey))
        true
      case Left(err) =>
        logger.warn(s"Sweep: failed to read lock file $path: ${err.message()}")
        false
      case Right(ObjectWithETag(IndexFile(_, Some(committedOffset), _), _))
          if committedOffset.value < masterOffset.value =>
        gcQueue.add(GcItem(bucket, path, tp, partitionKey))
        true
      case Right(ObjectWithETag(IndexFile(_, None, None), _)) =>
        // Empty lock created by ensureGranularLock but never committed to.
        // classifyLockFile already enforced the age threshold and cache absence,
        // so no active writer is using this file and it carries no durable data.
        // Without this branch, empty locks accumulate indefinitely under
        // high-cardinality PARTITIONBY with churning keys.
        gcQueue.add(GcItem(bucket, path, tp, partitionKey))
        true
      case _ =>
        false
    }

  override def suspendBackgroundWork(): Unit = {
    acceptingWork = false
    logger.debug(s"[${connectorTaskId.show}] Background work suspended (acceptingWork=false)")
  }

  /**
   * Shuts down background executors and performs a final synchronous GC drain.
   *
   * Ordering matters: the sweep executor is stopped first (it can enqueue new GC items),
   * then the GC executor, then a final synchronous `drainGcQueue()` flushes any items
   * that were enqueued but not yet drained. `seekedOffsets` must still be populated at
   * this point so the drain can distinguish owned partitions from revoked ones.
   *
   * The final `drainGcQueue()` call is direct (not through the scheduled lambda), so
   * it bypasses the `acceptingWork` gate and runs regardless of the flag's value.
   */
  override def close(): Unit = {
    val hadExecutors = executorsStarted || gcExecutor.isDefined || sweepExecutorOpt.isDefined
    if (!hadExecutors) {
      logger.info(s"IndexManagerV2 closed for ${connectorTaskId.show} (executors were never started)")
      return
    }
    // 1. Stop the orphan sweep first -- it can enqueue new GC items
    sweepExecutorOpt.foreach { exec =>
      exec.shutdownNow()
      try {
        val _ = exec.awaitTermination(5, TimeUnit.SECONDS)
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    }
    // 2. Stop the periodic GC drain timer
    gcExecutor.foreach { exec =>
      exec.shutdownNow()
      try {
        val _ = exec.awaitTermination(5, TimeUnit.SECONDS)
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    }
    // 3. Final synchronous drain to flush any remaining enqueued items
    drainGcQueue()
    // 4. Reset executor state so startExecutors() creates fresh executors if open() is called
    //    again on this instance (e.g. after a rebalance close → open cycle).
    executorsStarted = false
    gcExecutor       = None
    sweepExecutorOpt = None
    logger.info(s"IndexManagerV2 closed for ${connectorTaskId.show}")
  }

  override def indexingEnabled: Boolean = true
}

object IndexManagerV2 {

  case class GranularCacheEntry(offset: Option[Offset], eTag: String)

  /** Bounded re-read budget when a legacy-lock eTag bump loses a race to a granular zombie. */
  val MaxLegacyBumpAttempts: Int = 3

  // Discriminates between GC items targeting the live `.lock` blob and items
  // targeting orphaned `.lock.tmp.<uuid>` residue from a crashed writer. The
  // cache-reclaim filter in `drainGcQueue` is correct only for `Lock`: a `.tmp`
  // path can never be the active lock for any writer, so a populated
  // `granularCache[tp][pk]` (the dominant orphan-`.tmp` scenario) does not
  // imply the `.tmp` is in use and must not block its deletion.
  private[seek] sealed trait GcKind
  private[seek] object GcKind {
    case object Lock      extends GcKind
    case object TmpOrphan extends GcKind
  }

  private[seek] case class GcItem(
    bucket:         String,
    path:           String,
    topicPartition: TopicPartition,
    partitionKey:   String,
    kind:           GcKind = GcKind.Lock,
    retryCount:     Int    = 0,
  )

  // Anchored at the END of the basename: `<partitionKey>.lock.tmp.<uuid>`.
  // The UUID class matches the format produced by `UUID.randomUUID().toString`
  // (see `ConnectorTaskId.lockUuid`) -- 32 hex chars + 4 dashes -- but is kept
  // permissive (`[0-9a-fA-F-]+`) to avoid coupling to a specific UUID
  // canonical form. The captured group is the true partition key, including
  // any literal `.` characters preserved by `WriterManager.sanitize` (which
  // URL-encodes but leaves `.` alone). A naive `contains(".lock.tmp.")` test
  // would misclassify a real `.lock` whose partitionKey contains `.lock.tmp.`
  // (e.g. `name=report.lock.tmp.archive.lock`) as a tmp orphan and delete it
  // -- TmpOrphan items bypass the cache-reclaim filter in `drainGcQueue`.
  private[seek] val TmpOrphanPattern = """^(.*)\.lock\.tmp\.[0-9a-fA-F-]+$""".r

  val MaxGcRetries: Int = 3

  /**
   * Bounded re-read attempts when a master-lock NoOverwrite create loses the race. One re-read
   * normally suffices — cloud storage is read-after-write consistent for the destination — so
   * the cap only guards against a pathological absent/present flicker before surfacing a fatal
   * error.
   */
  val MaxOpenCreateRaceAttempts: Int = 3

  /**
   * Internal, recoverable signal: a master-lock NoOverwrite create lost the race because the
   * lock already exists. It NEVER escapes `IndexManagerV2.open` — `decideOpen` either resolves
   * it with a bounded re-read (adopting the winner's lock, including any PendingState recovery)
   * or converts it to a `FatalCloudSinkError` on exhaustion.
   */
  private[seek] case class LostCreateRaceError(topicPartition: TopicPartition) extends SinkError {
    override def exception():       Option[Throwable]   = Option.empty
    override def message():         String              = s"lost master-lock create race for $topicPartition"
    override def rollBack():        Boolean             = false
    override def topicPartitions(): Set[TopicPartition] = Set(topicPartition)
  }

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
   * Generates the cloud storage path for a granular lock file.
   * Layout: `<indexDir>/<connector>/.locks/<topic>/<partition>/<partitionKey>.lock`
   */
  private[seek] def generateGranularLockFilePath(
    connectorTaskId:   ConnectorTaskId,
    topicPartition:    TopicPartition,
    partitionKey:      String,
    directoryFileName: String,
  ): String =
    s"$directoryFileName/${connectorTaskId.name}/.locks/${topicPartition.topic}/${topicPartition.partition}/$partitionKey.lock"

  /**
   * Generates the cloud storage path for the orphan sweep marker file.
   * Layout: `<indexDir>/<connector>/.locks/<topic>/<partition>/sweep-marker.json`
   */
  private[seek] def generateSweepMarkerPath(
    connectorTaskId:   ConnectorTaskId,
    topicPartition:    TopicPartition,
    directoryFileName: String,
  ): String =
    s"$directoryFileName/${connectorTaskId.name}/.locks/${topicPartition.topic}/${topicPartition.partition}/sweep-marker.json"

  /**
   * Test-only access point for the pre-write master-lock barrier seam. Any production
   * reference to `IndexManagerV2TestHooks.*` is a bug; the name makes stray uses obvious
   * under code review. See the `testHookPreWriteMasterLockBarrier` field doc for semantics.
   */
  object IndexManagerV2TestHooks {
    def installPreWriteMasterLockBarrier(im: IndexManagerV2, barrier: () => Unit): Unit =
      im.testHookPreWriteMasterLockBarrier = barrier

    def clearPreWriteMasterLockBarrier(im: IndexManagerV2): Unit =
      im.testHookPreWriteMasterLockBarrier = () => ()
  }

}
