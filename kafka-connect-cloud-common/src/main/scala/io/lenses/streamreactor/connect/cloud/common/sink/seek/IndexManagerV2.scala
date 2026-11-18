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
import io.lenses.streamreactor.connect.cloud.common.storage.ConditionalDelete
import io.lenses.streamreactor.connect.cloud.common.storage.DeleteOutcome
import io.lenses.streamreactor.connect.cloud.common.storage.EmptyFileError
import io.lenses.streamreactor.connect.cloud.common.storage.FileDeleteError
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
import scala.util.Try
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
  batchCommitLocks:             BatchCommitLocks = BatchCommitLocks.NoOp,
  // Wall-clock source for the ownership-epoch floor. Injectable so tests are deterministic.
  clock: () => Long = () => System.currentTimeMillis(),
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

  // A smaller budget would let every sweep pass read no lock (one read is kept for the master
  // re-check) and so delete nothing. `IndexSettings` raises a configured value below the minimum.
  require(
    gcSweepMaxReads >= IndexManagerV2.MinGcSweepMaxReads,
    s"gcSweepMaxReads ($gcSweepMaxReads) must be >= ${IndexManagerV2.MinGcSweepMaxReads}",
  )

  private val lockOwner = connectorTaskId.lockUuid

  // Thread-safe map storing the latest offset for each TopicPartition seeked during SinkTask initialization.
  // Must be concurrent because open() uses parTraverse to process partitions on multiple fibers.
  private val seekedOffsets = TrieMap.empty[TopicPartition, Offset]

  // Thread-safe map tracking the latest eTags for index files, enabling conditional writes.
  // Must be concurrent because open() uses parTraverse to process partitions on multiple fibers.
  // Visibility: private[seek] so tests in this package can observe eTag-cache invariants.
  private[seek] val topicPartitionToETags = TrieMap.empty[TopicPartition, String]

  // LC-349: the partitions this instance currently holds -- the batch temp sweep's ownership check
  // (before its pre-LIST master read and again per delete chunk). Added at the very start of
  // `open(tp)` (before recovery and the open() sweep) and cleared by `close()`. Removed first thing
  // by `closePartitions` (CloudSinkTask.close(partitions), called with exactly the revoked
  // partitions), and by `clearTopicPartitionState` on a failed-open rollback.
  // Neither `seekedOffsets` ("has a committed offset") nor `acceptingWork` (false throughout
  // open()) means "currently held". Concurrent: open() adds from parallel fibers and the sweep
  // thread reads it.
  private[seek] val ownedPartitions: java.util.Set[TopicPartition] = ConcurrentHashMap.newKeySet[TopicPartition]()

  // Ownership epoch this instance holds for each owned TopicPartition. Set once by a successful ownership write in `open()` and cleared with the
  // rest of the partition state; never changed while the partition is owned, so commit-pool threads
  // and the GC/sweep threads all see the same value. Concurrent because open() uses parTraverse.
  private val tpEpoch = TrieMap.empty[TopicPartition, Long]

  /** The ownership epoch held for `topicPartition`, if any (read-only view for tests). */
  private[seek] def heldEpochOf(topicPartition: TopicPartition): Option[Long] = tpEpoch.get(topicPartition)

  // Partitions this instance has found itself fenced on: it read a lock written under a newer
  // ownership epoch, or lost a master-lock write to a newer owner. Concurrent: marked from the task
  // thread, commit-pool threads and the GC/sweep threads. The task fails fast while it is non-empty.
  // The value is the first fence seen against the latest held epoch: that epoch, and its reason (the
  // epochs involved) for the task-failure message. The epoch scopes the fence to one ownership
  // episode: a re-open lifts only the fences placed against an earlier epoch (`ownershipTaken`).
  private val fencedTps =
    new java.util.concurrent.ConcurrentHashMap[TopicPartition, Fence]()

  // Only partitions this instance holds an epoch for: a fence left on a partition it has since
  // closed is not this task's to fail on.
  override def fencedPartitions: Set[TopicPartition] =
    fencedTps.keySet().asScala.toSet.filter(tpEpoch.contains)

  override def isFenced(topicPartition: TopicPartition): Boolean = fencedTps.containsKey(topicPartition)

  override def fencingReason(topicPartition: TopicPartition): Option[String] =
    Option(fencedTps.get(topicPartition)).map(_.reason)

  /**
   * Marks a partition fenced when `heldEpoch` -- the epoch the fencing decision was taken under -- is
   * still the one held. A decision taken on a read that a close or a fresh ownership write has since
   * overtaken no longer applies, so it changes nothing: the epoch check and the update run
   * atomically against the removals (both lock the same map entry). Against the held epoch, a fence
   * placed against an earlier epoch is replaced; one already placed against this epoch keeps its
   * first reason; one placed against a later epoch is kept. A close clears the epoch before it
   * removes every fence; a re-open publishes its new epoch before it removes the fences placed
   * against an earlier one, so a fence placed against the new epoch in between survives, even when a
   * fence from the previous episode was still up (a re-open without a close). Returns whether the
   * partition is now fenced. The metric counts fencing episodes: once per fence placed against an
   * epoch, so a partition fenced, re-owned and fenced again counts twice.
   */
  private def markFenced(topicPartition: TopicPartition, heldEpoch: Long, reason: String): Boolean = {
    var placed = false
    val now = fencedTps.compute(
      topicPartition,
      (_, current) =>
        if (!tpEpoch.get(topicPartition).contains(heldEpoch)) current
        else if (current == null || current.epoch < heldEpoch) { placed = true; Fence(heldEpoch, reason) }
        else current,
    )
    if (placed) metrics.incrementPartitionsFenced()
    now != null
  }

  /**
   * The epoch an ownership write takes over a lock that carried `previous`: one more than
   * `previous`, floored by wall-clock time. Between new-version owners the `+ 1` makes it strictly
   * increasing whatever the clocks say; after an older connector version dropped the field
   * (`previous = None`), the clock keeps it above every epoch written before.
   */
  private def nextEpoch(previous: Option[Long]): Long =
    math.max(previous.getOrElse(0L) + 1L, clock())

  /**
   * The ownership epoch every lock write by this instance stamps. Set by
   * `open()`; a write without one is a bug (a path that skipped `open()`, or state cleared under a
   * live writer), so it fails fatally rather than writing an unstamped lock that would erase the
   * fence for the next reader.
   */
  private def heldEpoch(topicPartition: TopicPartition): Either[SinkError, Long] =
    if (isFenced(topicPartition))
      // Once fenced, this instance writes nothing more for the partition, on any key: a key the
      // newer owner has not touched yet would otherwise still accept the zombie's write.
      fencedRefusal(topicPartition, "a lock write").asLeft
    else
      tpEpoch.get(topicPartition).toRight(
        FatalCloudSinkError(
          s"No ownership epoch held for $topicPartition; refusing an unstamped lock write",
          topicPartition,
        ),
      )

  /** The error every refusal on a partition already marked fenced returns. */
  private def fencedRefusal(topicPartition: TopicPartition, refused: String): SinkError =
    FatalCloudSinkError(
      s"Fenced on $topicPartition (${fencingReason(topicPartition).getOrElse("a newer owner holds it")}); " +
        s"refusing $refused",
      topicPartition,
    )

  /**
   * Once a partition is marked fenced, this instance loads, recovers and serves no key of it, cached
   * or not: a key whose lock still carries this instance's epoch would otherwise pass the read rule
   * and have its pending chain driven (deleting data objects) before any write guard fires.
   */
  private def refuseIfFenced(topicPartition: TopicPartition, partitionKey: String): Either[SinkError, Unit] =
    if (isFenced(topicPartition)) fencedRefusal(topicPartition, s"to load lock $partitionKey").asLeft
    else ().asRight

  /**
   * The ownership-epoch read rule. A granular lock carrying a higher epoch
   * than the one this instance holds was written by a newer owner of the partition, so this
   * instance has been fenced: it must not cache the lock's token (that would hand a zombie the new
   * owner's fencing token), must not drive its pending chain, and must not write. The partition is
   * marked fenced so the task fails fast. Locks at or below our epoch pass unchanged; a missing
   * epoch reads as 0. Every load first requires a held epoch (`heldEpoch`, before the read): a
   * partition this instance does not hold (never opened, or closed) is not ours, so nothing of it
   * is read, cached or recovered. The rule itself refuses a lock read with no held epoch too, fatally
   * (every caller checks first today; this keeps a future caller, or state cleared during the read,
   * from accepting a lock it has nothing to compare against). A partition already marked fenced
   * refuses every lock, checked again here after the read: another thread (a commit, GC or sweep)
   * may have fenced it while the lock was read.
   */
  private def rejectIfFenced(
    topicPartition: TopicPartition,
    partitionKey:   String,
    lock:           IndexFile,
  ): Either[SinkError, Unit] =
    refuseIfFenced(topicPartition, partitionKey).flatMap(_ => rejectNewerEpoch(topicPartition, partitionKey, lock))

  private def rejectNewerEpoch(
    topicPartition: TopicPartition,
    partitionKey:   String,
    lock:           IndexFile,
  ): Either[SinkError, Unit] =
    tpEpoch.get(topicPartition) match {
      case Some(mine) if lock.epoch.getOrElse(0L) > mine =>
        val theirs = lock.epoch.getOrElse(0L)
        val _      = markFenced(topicPartition, mine, s"lock $partitionKey epoch $theirs > held epoch $mine")
        logger.error(
          s"[${connectorTaskId.show}] Fenced on $topicPartition: granular lock $partitionKey carries epoch $theirs " +
            s"(owner ${lock.owner}) > this instance's epoch $mine (lockUuid $lockOwner). A newer owner holds the " +
            s"partition; refusing to load or write it.",
        )
        FatalCloudSinkError(
          s"Fenced on $topicPartition: lock $partitionKey epoch $theirs > held epoch $mine",
          topicPartition,
        ).asLeft
      case Some(_) => ().asRight
      case None =>
        FatalCloudSinkError(
          s"No ownership epoch held for $topicPartition; refusing to accept lock $partitionKey",
          topicPartition,
        ).asLeft
    }

  /**
   * Records the epoch an ownership write in `open()` just landed, then passes the write through.
   * Takes the epoch we WROTE rather than reading it back from the storage response, so the held
   * epoch never depends on how a backend echoes the written object.
   */
  private def ownershipTaken(
    topicPartition: TopicPartition,
    epoch:          Long,
    written:        ObjectWithETag[IndexFile],
    previous:       Option[Long],
  ): ObjectWithETag[IndexFile] = {
    val held = tpEpoch.put(topicPartition, epoch)
    testHookOwnershipPublished(topicPartition, epoch) // test seam — no-op in production
    // A fresh ownership write re-takes the partition above every epoch seen so far, so a fence
    // from an earlier ownership episode (e.g. before a rebalance re-assigned it here) no longer
    // applies. GC deletes queued in that episode were selected against locks another owner may
    // since have rewritten at the same path, so they are dropped rather than drained under the new
    // ownership whenever another owner may have held the partition in between: this instance was
    // fenced, it held no epoch (the partition was closed, which clears the epoch, so who held it
    // since is unknown; a drain or sweep run in flight at the close can still have re-queued
    // items), or the lock no longer carries the epoch it held (a handover it never noticed). Only a
    // re-open without a close, over a lock still carrying the held epoch, keeps the queue.
    // Only the fences placed against an earlier epoch are lifted. Once the new epoch is published a
    // fencing check (a sweep or commit thread) can fence against it -- a newer owner took the
    // partition again during this re-open -- and that fence must survive.
    val handedOver = held.isEmpty || previous != held
    dropFence(topicPartition, dropQueue = handedOver, below = Some(epoch))
    written
  }

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

  /**
   * Clears `tp`'s fence, if any, together with the GC items queued for it when it was fenced or
   * `dropQueue` is set: those were selected while another owner may have held the partition.
   * `below = Some(e)` lifts only a fence placed against an epoch lower than `e` (a re-open that
   * published `e`), checked and removed atomically in the map entry; `None` lifts any fence (a close).
   */
  private def dropFence(tp: TopicPartition, dropQueue: Boolean, below: Option[Long]): Unit = {
    // Purge before lifting the fence: while it is up, a concurrent drain discards what it polls.
    if (dropQueue || fencedTps.containsKey(tp)) {
      val _ = gcQueue.removeIf(_.topicPartition == tp)
      metrics.setGcQueueDepth(gcQueue.size())
    }
    below match {
      case None        => val _ = fencedTps.remove(tp)
      case Some(epoch) => val _ = fencedTps.computeIfPresent(tp, (_, fence) => if (fence.epoch < epoch) null else fence)
    }
  }

  // Exposed for testing only; not part of the public API.
  private[seek] def gcQueueSize: Int = gcQueue.size()

  // Best-effort gate: when false, scheduled invocations of drainGcQueue / sweepOrphanedLocks
  // are skipped. Set to false by suspendBackgroundWork() (called from CloudSinkTask.close()),
  // set to true at the end of open(), and again at the end of closePartitions() when this
  // instance still holds a partition (a revoke-only cooperative rebalance has no open()). This cannot stop an already-running invocation -- the
  // volatile provides visibility, not mutual exclusion. In-flight executions stay safe because
  // every lock delete is conditional on the version GC selected:
  //  - drainGcQueue: deletes items selected before the rebalance as obsolete (offset below
  //    globalSafeOffset). If the partition is handed over meanwhile and the new owner takes a
  //    selected lock over (or rewrites it) before the delete lands, the store refuses the delete
  //    and the lock stays. A lock the new owner has not touched is below the floor it inherits.
  //  - sweepOrphanedLocks: LIST/GET plus a sweep-marker write; the GcItems it enqueues for a
  //    revoked partition are dropped when its close clears its state, or discarded by the next drain.
  //    A run still in flight can re-queue them after that; their deletes are conditional too.
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
  private[sink] def sweepAllBatchTemps(): Unit =
    // Stop visiting partitions once interrupted (close() -> shutdownNow): a sweep that was waiting
    // on a partition's commit lock restores the flag and must not move on to the next partition.
    for (tp <- seekedOffsets.keys.toList if !Thread.currentThread().isInterrupted && !isFenced(tp)) {
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

    // Only the partitions given are touched. Kafka Connect passes open() the newly assigned
    // partitions only (on a cooperative rebalance, partitions kept across it are not passed
    // again), so a partition missing here is not a revoked one: revoked partitions arrive through
    // close(partitions) and are released by `closePartitions`.
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
        topicPartitions.foreach(releasePartition)
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

    // LC-349: held from here on, so the open() sweep below passes the per-chunk ownership check
    // even for a partition with no committed offset and while `acceptingWork` is still false.
    // A failed open() rolls this back via `clearTopicPartitionState`.
    val _    = ownedPartitions.add(topicPartition)
    val path = generateLockFilePath(connectorTaskId, topicPartition, directoryFileName)
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      firstRead        = tryOpen(bucketAndPrefix.bucket, path)
      offset <- decideOpen(
        topicPartition,
        path,
        bucketAndPrefix,
        firstRead,
        MaxOpenCreateRaceAttempts,
        observedEpoch = readEpoch(firstRead),
        lastWrite     = None,
      )
      // Batch mode only, strictly after master resolution and the ownership bump: fence every
      // legacy granular lock left by a granular-mode deployment and purge them once resolved.
      // Entirely best-effort -- see `snapshotLegacyLocks` -- it never fails `open()`.
      _ = if (commitMode == CommitMode.Batch) snapshotLegacyLocks(topicPartition, bucketAndPrefix)
      // Batch mode only, strictly after legacy resolution: reap orphaned batch temp objects.
      // Best-effort — never fails open. Same per-chunk checks as the periodic sweep (LC-349).
      _ = if (commitMode == CommitMode.Batch && !isFenced(topicPartition))
        sweepBatchTemps(topicPartition, bucketAndPrefix)
    } yield offset

  }

  /**
   * Deletes orphaned batch temp objects under this task's connector-scoped prefix
   * `.temp-upload/<connector>/<topic>/<partition>/`. An orphan is any object older than
   * `gcSweepMinAgeSeconds` that is not the source of a `CopyOperation` still referenced by the
   * master lock's `PendingState`. Never touches paths outside the prefix -- granular temps live
   * under `.temp-upload/<topic>/<partition>/` and other connectors under their own name. Failures
   * are logged, never fatal.
   *
   * LC-349 (M'): the master read, the LIST and the age/reference filter run WITHOUT the partition's
   * [[BatchCommitLocks]] lock. Each delete chunk then runs under it, and re-checks everything a
   * commit could have changed since the LIST -- see [[deleteChunkUnderCommitLock]]. A commit
   * therefore ordinarily waits for one master GET plus one `deleteFiles` chunk, never a whole pass.
   * In the ambiguous case of `sweepFenceReason` (an older-version lock with no epoch, read across a
   * re-take) the chunk reads the master once more, so it waits for at most two GETs.
   * The same checks apply at `open()` and on the periodic sweep.
   */
  private def sweepBatchTemps(topicPartition: TopicPartition, bucketAndPrefix: CloudLocation): Unit =
    if (!ownedPartitions.contains(topicPartition)) {
      // Released or revoked: skip the master read and the LIST too, not just the deletes. The
      // per-chunk check in `deleteChunkUnderCommitLock` still covers a release that lands mid-pass.
      logger.debug(
        s"[${connectorTaskId.show}] Batch temp sweep skipped for $topicPartition: partition not held by this task",
      )
    } else
      try {
        val prefix       = s".temp-upload/${connectorTaskId.name}/${topicPartition.topic}/${topicPartition.partition}/"
        val ageThreshold = Instant.now().minusSeconds(gcSweepMinAgeSeconds.toLong)
        // Exclusion: any Copy source still recorded in the master lock's PendingState. On the
        // at-`open()` path this is purely defensive (a resolved open leaves pendingState = None),
        // but on the periodic `sweepAllBatchTemps` path it keeps a live commit's in-flight Copy
        // sources out of the orphan set, so an unreadable lock must NOT be read as "nothing is
        // referenced". Fail closed and skip the cycle, exactly as `isSweepDueForPartition` does for
        // the sweep marker; the next sweep retries.
        val masterPath = generateLockFilePath(connectorTaskId, topicPartition, directoryFileName)
        // The epoch held when the master is read: the fencing decision is taken against it.
        val heldAtRead = tpEpoch.get(topicPartition)
        readMasterForSweep(bucketAndPrefix.bucket, masterPath) match {
          case Left(err) =>
            logger.warn(
              s"[${connectorTaskId.show}] Batch temp sweep skipped for $topicPartition: master lock unreadable, " +
                s"cannot tell in-flight Copy sources from orphans: ${err.message()}",
            )
          case Right(master) =>
            sweepFenceReason(topicPartition, bucketAndPrefix.bucket, masterPath, master, heldAtRead) match {
              case Some(reason) =>
                logger.warn(s"[${connectorTaskId.show}] Batch temp sweep for $topicPartition: $reason")
              case None =>
                sweepOrphansUnder(
                  topicPartition,
                  bucketAndPrefix,
                  masterPath,
                  prefix,
                  ageThreshold,
                  referencedCopySources(master),
                )
            }
        }
      } catch {
        case NonFatal(e) =>
          logger.warn(s"[${connectorTaskId.show}] Batch temp sweep threw for $topicPartition; ignoring", e)
      }

  /**
   * The ownership rule of `confirmMasterFenced`, applied to a master lock the batch temp sweep has
   * just read. Ordinarily that costs no extra GET; the ambiguous case below (an older-version lock
   * with no epoch, read across a re-take) costs at most one extra GET per decision, which a delete
   * chunk issues while it holds the partition's commit lock. The temp prefix is shared by every
   * instance of this task, so a sweep under a newer owner's master would delete its
   * uploaded-but-not-yet-recorded temps.
   * `Some(reason)` means skip: no ownership epoch is held for the partition, or the lock was
   * written by another instance or under a higher epoch (which also marks the partition fenced).
   * No lock, or a 0-byte one, carries no owner, so it passes, as before.
   *
   * Applied to the pre-LIST read and again to each delete chunk's fresh read. It is a guard, not
   * complete fencing: a takeover between a chunk's read and its by-path delete is not seen by that
   * delete; the age threshold keeps a new owner's fresh temps out of it.
   *
   * `heldAtRead` is the epoch held when `master` was read. A close or a re-open by this instance can
   * land between the read and the decision, so the decision is taken against the epoch held now:
   *  - none (the partition was closed): skip, without fencing.
   *  - unchanged since the read: fence on another owner or a higher epoch, as above.
   *  - changed, and the lock carries an epoch above the one held now: no write of this instance
   *    explains it, so it fences against the epoch held now.
   *  - changed, and the lock carries an epoch at or below the one held now: the read predates this
   *    instance's own re-take, so it skips the cycle without fencing.
   *  - changed, and the lock carries no epoch but another owner: the read cannot tell whether it
   *    predates the re-take, so the lock is read once more and judged by the same rules. A fresh
   *    read that does not fence still skips the cycle.
   * The fence is placed only while the epoch it was decided against is still held (`markFenced`).
   */
  private def sweepFenceReason(
    topicPartition: TopicPartition,
    bucket:         String,
    masterPath:     String,
    master:         Option[IndexFile],
    heldAtRead:     Option[Long],
    mayReRead:      Boolean = true,
  ): Option[String] =
    master.flatMap { m =>
      val theirs = m.epoch.getOrElse(0L)
      heldAtRead match {
        case None =>
          "no ownership epoch is held for the partition; batch temp sweep skipped".some
        case Some(mine) =>
          tpEpoch.get(topicPartition) match {
            case None                                             => retakenWhileRead
            case Some(now) if theirs > now                        => fenceOnSweep(topicPartition, m, now)
            case Some(now) if now == mine && m.owner != lockOwner => fenceOnSweep(topicPartition, m, now)
            case Some(now) if now == mine                         => None
            case Some(_) if m.epoch.isEmpty && m.owner != lockOwner && mayReRead =>
              val heldAtReRead = tpEpoch.get(topicPartition)
              readMasterForSweep(bucket, masterPath) match {
                case Left(err) =>
                  s"master lock unreadable on the re-read; batch temp sweep skipped: ${err.message()}".some
                case Right(fresh) =>
                  sweepFenceReason(topicPartition, bucket, masterPath, fresh, heldAtReRead, mayReRead = false)
                    .orElse(retakenWhileRead)
              }
            case Some(_) => retakenWhileRead
          }
      }
    }

  /** Fences the partition on a master lock the sweep read, when `held` is still the epoch held. */
  private def fenceOnSweep(topicPartition: TopicPartition, m: IndexFile, held: Long): Option[String] = {
    val theirs = m.epoch.getOrElse(0L)
    if (markFenced(topicPartition, held, s"master lock owner ${m.owner} epoch $theirs, held epoch $held")) {
      logger.error(
        s"[${connectorTaskId.show}] Fenced on $topicPartition: the batch temp sweep read a master lock " +
          s"written by another instance (owner ${m.owner}, epoch $theirs; " +
          s"this instance's lockUuid $lockOwner, epoch $held). The task will fail on its next put().",
      )
      (s"lock owned by another instance; batch temp sweep skipped (owner=${m.owner}, " +
        s"epoch=$theirs, this instance=$lockOwner, held epoch=$held)").some
    } else retakenWhileRead
  }

  private def retakenWhileRead: Option[String] =
    "the partition was closed or re-taken while its master lock was read; batch temp sweep skipped".some

  /**
   * Reads the master lock for the sweep. `Right(None)` when there is no lock, or a 0-byte lock
   * that cannot carry a PendingState by construction (see the EmptyFileError arm of `decideOpen`):
   * nothing can be referenced. Any other failure is a `Left`, and the caller fails closed.
   */
  private def readMasterForSweep(bucket: String, masterPath: String): Either[FileLoadError, Option[IndexFile]] =
    tryOpen(bucket, masterPath) match {
      case Right(master) => master.wrappedObject.some.asRight
      case Left(_: FileNotFoundError) => Option.empty[IndexFile].asRight
      case Left(_: EmptyFileError) => Option.empty[IndexFile].asRight
      case Left(err) => err.asLeft
    }

  private def referencedCopySources(master: Option[IndexFile]): Set[String] =
    master.toList
      .flatMap(_.pendingState.toList)
      .flatMap(_.pendingOperations.toList)
      .collect { case c: CopyOperation => c.source }
      .toSet

  /**
   * LIST-and-delete half of [[sweepBatchTemps]]: deletes, chunk by chunk, every object under
   * `prefix` older than `ageThreshold` that is not in `referenced` and that the per-chunk fresh
   * read does not reference either.
   */
  private def sweepOrphansUnder(
    topicPartition:  TopicPartition,
    bucketAndPrefix: CloudLocation,
    masterPath:      String,
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
          // partial failure just leaves the failed chunk's temps for the next sweep. The chunk is
          // also the unit of the commit lock (LC-349): it bounds how long a commit can wait.
          var deleted = 0
          var failed  = 0
          val chunks  = orphans.grouped(gcBatchSize)
          var proceed = true
          try {
            while (proceed && chunks.hasNext) {
              val chunk = chunks.next()
              batchCommitLocks.withLockInterruptibly(topicPartition) {
                deleteChunkUnderCommitLock(topicPartition, bucketAndPrefix.bucket, masterPath, chunk)
              } match {
                case ChunkOutcome.Stop => proceed = false
                case ChunkOutcome.Done(d, f) =>
                  deleted += d
                  failed += f
              }
            }
          } catch {
            case _: InterruptedException =>
              // shutdownNow() while waiting behind a commit: no delete was issued for this chunk.
              // Restore the flag so the executor (and sweepAllBatchTemps) see the interrupt.
              Thread.currentThread().interrupt()
              logger.info(
                s"[${connectorTaskId.show}] Batch temp sweep for $topicPartition interrupted while waiting for " +
                  "the partition's commit lock; stopping",
              )
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
   * One delete chunk of the batch temp sweep. MUST be called holding `topicPartition`'s
   * [[BatchCommitLocks]] lock, and is fully synchronous (no `IO`), so the lock never spans an
   * effect boundary.
   *
   * Why it is safe (LC-349): while the lock is held no `commitBatch` is in flight on this
   * partition, so a temp listed earlier is either from a commit that finished (already moved;
   * deleting a missing key is a no-op), from one that failed before its CAS (a true orphan: the
   * retry re-stages under a new batch id), or from one that failed after its CAS (referenced by
   * the fresh read below, so dropped from the chunk). Temps uploaded after the LIST are not in it.
   *
   * The owner fence keeps a stale or zombie instance -- which shares this temp prefix -- from
   * deleting the current owner's temps: `open()` always rewrites the owner to its own `lockOwner`
   * and takes a new ownership epoch, and a lock showing either marks this instance fenced (see
   * `sweepFenceReason`).
   *
   * The ownership check is [[ownedPartitions]], not `acceptingWork`/`seekedOffsets`: those mean
   * "not suspended" and "has a committed offset", and both are false for a brand-new partition
   * during its own `open()`.
   */
  private def deleteChunkUnderCommitLock(
    topicPartition: TopicPartition,
    bucket:         String,
    masterPath:     String,
    chunk:          Seq[String],
  ): ChunkOutcome =
    if (!ownedPartitions.contains(topicPartition)) {
      logger.debug(
        s"[${connectorTaskId.show}] Batch temp sweep for $topicPartition stopped: partition no longer held " +
          "by this task",
      )
      ChunkOutcome.Stop
    } else {
      val heldAtRead = tpEpoch.get(topicPartition)
      readMasterForSweep(bucket, masterPath) match {
        case Left(err) =>
          logger.warn(
            s"[${connectorTaskId.show}] Batch temp sweep stopped for $topicPartition: master lock unreadable on " +
              s"the pre-delete read, cannot tell in-flight Copy sources from orphans: ${err.message()}",
          )
          ChunkOutcome.Stop
        // `None` (no lock, or a 0-byte lock) references nothing, so the chunk is deleted, as before
        // LC-349. That is safe: temp paths are unique per attempt (they carry a random batch id),
        // an in-flight commit on this partition would be holding the lock we hold now, and no
        // other process can commit -- and so reference a temp -- without the master lock. An
        // unreferenced temp is therefore a true orphan. Every other read error fails closed above.
        // A lock held by another instance, or under a newer epoch, fences this one: same rule as
        // the pre-LIST read.
        case Right(master) =>
          sweepFenceReason(topicPartition, bucket, masterPath, master, heldAtRead) match {
            case Some(reason) =>
              logger.warn(s"[${connectorTaskId.show}] Batch temp sweep for $topicPartition: $reason")
              ChunkOutcome.Stop
            case None =>
              val nowReferenced = referencedCopySources(master)
              val toDelete      = chunk.filterNot(nowReferenced.contains)
              if (toDelete.isEmpty) ChunkOutcome.Done(0, 0)
              else
                storageInterface.deleteFiles(bucket, toDelete) match {
                  case Left(err) =>
                    logger.warn(
                      s"[${connectorTaskId.show}] Batch temp sweep failed to delete ${toDelete.size} orphan(s) for " +
                        s"$topicPartition: ${err.message()}",
                    )
                    ChunkOutcome.Done(0, toDelete.size)
                  case Right(_) => ChunkOutcome.Done(toDelete.size, 0)
                }
          }
      }
    }

  /**
   * Decides how to open a master lock given the result of reading it, and is re-entrant so a
   * failed write (a lost NoOverwrite create race, any other failed create, or a failed bump) can
   * re-read the lock and route it through the
   * SAME arms (crucially, a re-read lock carrying a PendingState flows through
   * `processPendingOperations`, not a naive eTag adoption — that is what makes recovery
   * correct, see the architecture note on the reverted create-race adoption fix).
   *
   * A re-read (after a lost write) is only routed back through the arms while it still carries the
   * ownership epoch this open first observed: writes that keep the epoch (the previous owner
   * advancing its floor) don't change who owns the partition, so retrying over them is legitimate.
   * Any other epoch can only come from another instance's ownership write after our first read,
   * and that instance is the one most recently assigned the partition: taking the lock back from
   * it would make this instance the zombie and fence the legitimate owner. The open fails instead
   * (see `openOwnershipLost`).
   *
   * Every failed create, ownership bump and pending-chain ownership bump is re-read, the last
   * included: the re-read after the last write still adopts this instance's landed write and still
   * refuses a newer owner, but with no writes left it decides nothing else, so anything else (the
   * lock unchanged, or unreadable) fails with that write's error. The 0-byte lock repair (the
   * EmptyFileError arm) is not re-read: a failed repair fails the open at once and the restart
   * re-opens.
   *
   * A re-read showing this instance's lockUuid at the epoch of any write this open made is that
   * write having landed, whichever of them it was, and is adopted. Every one of them is conditional
   * (on a version, or on absence), so once one lands the others fail.
   *
   * @param tryOpenResult The result of `tryOpen` for this lock path.
   * @param attemptsLeft  Writes this open may still make (shared by the create, bump and pending-chain
   *                      bump arms); 0 on the re-read after the last one.
   * @param observedEpoch The epoch of this open's first read (0 if absent, 0-byte or unstamped).
   * @param lastWrite     The write that just failed, with every epoch this open has written; `None`
   *                      on the first read.
   */
  private def decideOpen(
    topicPartition:  TopicPartition,
    path:            String,
    bucketAndPrefix: CloudLocation,
    tryOpenResult:   Either[FileLoadError, ObjectWithETag[IndexFile]],
    attemptsLeft:    Int,
    observedEpoch:   Long,
    lastWrite:       Option[FailedOpenWrite],
  ): Either[SinkError, Option[Offset]] = {

    // The write at `epoch` failed: it joins every epoch this open wrote before it.
    def failedWrite(epoch: Long, spent: SinkError): Option[FailedOpenWrite] =
      Some(FailedOpenWrite(lastWrite.fold(Set.empty[Long])(_.writtenEpochs) + epoch, spent))

    tryOpenResult match {

      // Checked first: our own landed write no longer carries the observed epoch, and is not a takeover.
      // Any write this open made can be the one that landed, not only the last: an earlier write that
      // failed on the client may land late and fail a retry stamped with a later clock. The owner
      // alone is not enough: this instance may have written the lock in an earlier ownership.
      case Right(reRead @ ObjectWithETag(lock, _))
          if lock.owner == lockOwner && lastWrite.exists(_.writtenEpochs.contains(lock.epoch.getOrElse(0L))) =>
        adoptOwnOpenWrite(topicPartition, path, reRead, lock.epoch.getOrElse(0L), observedEpoch)

      case Right(ObjectWithETag(lock, _)) if lastWrite.isDefined && lock.epoch.getOrElse(0L) != observedEpoch =>
        openOwnershipLost(topicPartition, path, observedEpoch, lock.epoch.getOrElse(0L), lock.owner).asLeft

      case Left(EmptyFileError(_, _)) if lastWrite.isDefined && observedEpoch != 0L =>
        openOwnershipLost(topicPartition, path, observedEpoch, 0L, "<0-byte lock>").asLeft

      // Every arm below either writes or reports the re-read: with the writes spent, fail as the last
      // write failed, and never write again.
      case reRead if attemptsLeft <= 0 =>
        val spent = lastWrite.map(_.spent).getOrElse(
          FatalCloudSinkError(s"Exhausted master-lock ownership writes for $topicPartition at $path", topicPartition),
        )
        val shows = reRead match {
          case Left(FileNotFoundError(_, _)) | Right(_) => "shows neither this instance's write nor a newer owner"
          case Left(readError)                          => s"failed (${readError.message()})"
        }
        logger.warn(
          s"[${connectorTaskId.show}] Master-lock ownership writes exhausted for $topicPartition at $path: the " +
            s"final re-read $shows; failing with the last write's error: ${spent.message()}",
        )
        spent.asLeft

      case Left(FileNotFoundError(_, _)) =>
        val epoch = nextEpoch(None)
        createNewIndexFileNoOverwrite(topicPartition, path, bucketAndPrefix, epoch) match {
          case Right(written) =>
            updateDataReturnOffset(topicPartition, ownershipTaken(topicPartition, epoch, written, None)).asRight[
              SinkError,
            ]

          case Left(_: LostCreateRaceError) =>
            // Another task won the create (or our own create landed with its response lost). The
            // lock now exists — re-read and re-decide through the same arms so the winner's offset
            // (and any PendingState) is adopted/recovered. If the writes are spent and the lock
            // oscillated absent/present, the open fails fatally so Connect restarts the task and
            // re-seeks from the durable lock.
            logger.info(
              s"[${connectorTaskId.show}] Lost master-lock create race for $topicPartition at $path; " +
                s"re-reading existing lock (writes left: ${attemptsLeft - 1})",
            )
            decideOpen(
              topicPartition,
              path,
              bucketAndPrefix,
              tryOpen(bucketAndPrefix.bucket, path),
              attemptsLeft - 1,
              observedEpoch,
              failedWrite(
                epoch,
                FatalCloudSinkError(
                  s"Exhausted master-lock create-race retries for $topicPartition at $path",
                  topicPartition,
                ),
              ),
            )

          case Left(other) =>
            // Any other failure (a timeout, a network error) may still have landed: re-read and
            // re-decide as for a failed bump, so a landed create is adopted, a newer owner's lock is
            // refused, and a still-absent lock is created again while writes are left. With the
            // writes spent the open fails with this error.
            logger.info(
              s"[${connectorTaskId.show}] Master-lock create failed for $topicPartition at $path; re-reading " +
                s"(writes left: ${attemptsLeft - 1}): ${other.message()}",
            )
            decideOpen(
              topicPartition,
              path,
              bucketAndPrefix,
              tryOpen(bucketAndPrefix.bucket, path),
              attemptsLeft - 1,
              observedEpoch,
              failedWrite(epoch, other),
            )
        }

      case Left(EmptyFileError(_, eTag)) =>
        // The master lock exists as a 0-byte poison blob — residue of Bug B's non-atomic write.
        // A length-0 blob cannot carry committedOffset or PendingState by construction
        // (the serialiser produces at minimum ~40 bytes for any non-null IndexFile).
        // Take ownership via setIfMatch(eTag) instead of setIfNoneMatch("*") — the file
        // exists, so a NoOverwrite write would always 412.
        // NOTE: pendingOperationsProcessors is NOT invoked here — the 0-byte file proves
        // no PendingState was ever durably recorded; the recovery write sets pendingState=None.
        val epoch = nextEpoch(None)
        val idx   = IndexFile(lockOwner, Option.empty, Option.empty, Some(epoch))
        storageInterface.writeBlobToFile(bucketAndPrefix.bucket, path, ObjectWithETag(idx, eTag))
          .leftMap { err: UploadError =>
            new FatalCloudSinkError(err.message(), err.toExceptionOption, topicPartition)
          }
          .map(written => updateDataReturnOffset(topicPartition, ownershipTaken(topicPartition, epoch, written, None)))

      case Left(fileLoadError: FileLoadError) =>
        new FatalCloudSinkError(fileLoadError.message(), fileLoadError.toExceptionOption, topicPartition).asLeft[
          Option[Offset],
        ]

      case Right(ObjectWithETag(
            IndexFile(_,
                      committedOffset,
                      Some(pendingStateFound @ PendingState(pendingOffset, pendingOperations)),
                      prevEpoch,
            ),
            eTag,
          )) =>
        // Ownership bump BEFORE driving the chain. Without this, a zombie that CAS'd the
        // PendingState can win the first chain step (its `update` succeeds on the stale eTag)
        // while we are still mid-recovery, so the fence never actually moves to ownership
        // change for this arm -- only the content-preserving owner rewrite does that. Rewriting
        // here (owner only; committedOffset/pendingState unchanged) seats the fence first, then
        // the chain is driven with the NEW eTag so our own first step is the one that wins any
        // race against a lingering zombie.
        val epoch = nextEpoch(prevEpoch)
        storageInterface.writeBlobToFile(
          bucketAndPrefix.bucket,
          path,
          ObjectWithETag(IndexFile(lockOwner, committedOffset, Some(pendingStateFound), Some(epoch)), eTag),
        ) match {
          case Left(err) =>
            metrics.incrementMasterLockFailures()
            logger.info(
              s"[${connectorTaskId.show}] Ownership bump lost a race while recovering a pending chain for " +
                s"$topicPartition at $path; re-reading (writes left: ${attemptsLeft - 1}): ${err.message()}",
            )
            decideOpen(
              topicPartition,
              path,
              bucketAndPrefix,
              tryOpen(bucketAndPrefix.bucket, path),
              attemptsLeft - 1,
              observedEpoch,
              failedWrite(epoch, new FatalCloudSinkError(err.message(), err.toExceptionOption, topicPartition)),
            )
          case Right(bumped) =>
            metrics.incrementMasterLockUpdates()
            recoverAfterBump(
              topicPartition,
              epoch,
              bumped,
              prevEpoch,
              committedOffset,
              PendingState(pendingOffset, pendingOperations),
            )
        }

      case Right(ObjectWithETag(IndexFile(_, committedOffset, None, prevEpoch), eTag)) =>
        // Ownership bump. Adopting the eTag without rewriting would leave the previous owner's
        // token valid until this task's first commit, so a zombie could still write the lock in
        // between. Rewriting here moves the fence to ownership change. Content is unchanged apart
        // from the owner field, so nothing a reader depends on moves.
        // A Left means another task raced us for ownership: re-read and re-decide through
        // the same arms (bounded by `attemptsLeft`, shared with the lost-create-race budget)
        // rather than failing `open()` on a single lost race -- the re-read might show the
        // winner's PendingState, which must still be recovered through the arm above, not
        // adopted naively.
        val epoch = nextEpoch(prevEpoch)
        storageInterface.writeBlobToFile(
          bucketAndPrefix.bucket,
          path,
          ObjectWithETag(IndexFile(lockOwner, committedOffset, None, Some(epoch)), eTag),
        ) match {
          case Right(written) =>
            metrics.incrementMasterLockUpdates()
            updateDataReturnOffset(topicPartition, ownershipTaken(topicPartition, epoch, written, prevEpoch)).asRight[
              SinkError,
            ]
          case Left(err) =>
            metrics.incrementMasterLockFailures()
            logger.info(
              s"[${connectorTaskId.show}] Master-lock ownership bump lost a race for $topicPartition at $path; " +
                s"re-reading (writes left: ${attemptsLeft - 1}): ${err.message()}",
            )
            decideOpen(
              topicPartition,
              path,
              bucketAndPrefix,
              tryOpen(bucketAndPrefix.bucket, path),
              attemptsLeft - 1,
              observedEpoch,
              failedWrite(epoch, new FatalCloudSinkError(err.message(), err.toExceptionOption, topicPartition)),
            )
        }

      case Right(objectWithetag @ ObjectWithETag(IndexFile(_, _, _, _), _)) =>
        updateDataReturnOffset(topicPartition, objectWithetag).asRight[SinkError]
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
   * Seats an ownership write over a lock carrying a PendingState, then drives the chain. The epoch
   * is seated first: every recovery write stamps it.
   */
  private def recoverAfterBump(
    topicPartition:  TopicPartition,
    epoch:           Long,
    bumped:          ObjectWithETag[IndexFile],
    previous:        Option[Long],
    committedOffset: Option[Offset],
    pendingState:    PendingState,
  ): Either[SinkError, Option[Offset]] = {
    val _ = ownershipTaken(topicPartition, epoch, bumped, previous)
    topicPartitionToETags.put(topicPartition, bumped.eTag)
    pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      committedOffset,
      pendingState,
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

  /**
   * A re-read in `open()` that shows this instance's `lockUuid` and the epoch of the write that
   * just failed is that write, landed with its response lost (e.g. a timeout whose SDK retry then
   * failed its precondition, or reported that the created object exists). `lockUuid` is unique to
   * this instance and the epoch was computed by this open, so no other writer can have produced
   * it. It is taken as the write's result, with no second ownership write: the lock is seated as
   * that arm's success branch seats it, and a PendingState it carries is driven before returning.
   */
  private def adoptOwnOpenWrite(
    topicPartition: TopicPartition,
    path:           String,
    reRead:         ObjectWithETag[IndexFile],
    epoch:          Long,
    observedEpoch:  Long,
  ): Either[SinkError, Option[Offset]] = {
    // The lock our write replaced carried the observed epoch (None when absent or unstamped).
    val previous = Some(observedEpoch).filter(_ != 0L)
    logger.warn(
      s"[${connectorTaskId.show}] Master-lock ownership write for $topicPartition at $path landed but its " +
        s"response was lost: the stored lock carries this instance's lockUuid and epoch $epoch, so it was adopted " +
        s"with no second write.",
    )
    reRead.wrappedObject.pendingState match {
      case Some(pending) =>
        recoverAfterBump(topicPartition, epoch, reRead, previous, reRead.wrappedObject.committedOffset, pending)
      case None =>
        updateDataReturnOffset(topicPartition, ownershipTaken(topicPartition, epoch, reRead, previous)).asRight
    }
  }

  /** The ownership epoch a master-lock read shows: 0 when absent, 0-byte, unreadable or unstamped. */
  private def readEpoch(read: Either[FileLoadError, ObjectWithETag[IndexFile]]): Long =
    read.toOption.flatMap(_.wrappedObject.epoch).getOrElse(0L)

  /**
   * The refusal when a re-read in `open()` shows that another instance took the partition over
   * after this open's first read. Nothing more is written and no pending chain is driven. The
   * partition is not marked fenced: the failed `open()` rolls every requested partition back
   * (which would clear the mark); the fatal error is what fails the task, under every error
   * policy, and the restart's open reads the newer epoch as its first read.
   */
  private def openOwnershipLost(
    topicPartition: TopicPartition,
    path:           String,
    observedEpoch:  Long,
    seenEpoch:      Long,
    seenOwner:      String,
  ): SinkError = {
    metrics.incrementMasterLockOpenOwnershipLost()
    logger.warn(
      s"[${connectorTaskId.show}] Ownership of $topicPartition was taken over during open(): the master lock at " +
        s"$path now carries epoch $seenEpoch (owner $seenOwner), not the epoch $observedEpoch this open first read " +
        s"(this instance's lockUuid $lockOwner). Not taking it back; failing the task so a restart re-opens.",
    )
    FatalCloudSinkError(
      s"Ownership of $topicPartition was taken over during open() (epoch $observedEpoch -> $seenEpoch, " +
        s"owner $seenOwner); not taking it back",
      topicPartition,
    )
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
    epoch:           Long,
  ): Either[SinkError, ObjectWithETag[IndexFile]] = {
    val idx = IndexFile(lockOwner, Option.empty, Option.empty, Some(epoch))
    storageInterface.writeBlobToFile(bucketAndPrefix.bucket, path, NoOverwriteExistingObject(idx))
      .leftMap {
        // Lost the create race (another task created the lock first). Surface a recoverable
        // signal so `decideOpen` re-reads and adopts the winner's lock instead of failing.
        case _: NonOverwriteFileExistsError => LostCreateRaceError(topicPartition): SinkError
        // Any other write failure (permissions, network, disk). It may still have landed, so
        // `decideOpen` re-reads before failing with it.
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
      epoch <- heldEpoch(topicPartition)
      index = ObjectWithETag(
        IndexFile(lockOwner, committedOffset, pendingState, Some(epoch)),
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
              // Once a read fences the partition, no further key is touched (not even read, nor a
              // pending chain's data operations): `loadLegacy` refuses before reading anything (its
              // `heldEpoch` check). The keys left count as unresolved, so nothing is purged.
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
   * `DeleteObjects` call) cannot permanently block a high-cardinality partition's purge.
   *
   * Each lock is deleted only at the version this instance's own fencing left it at (the token
   * `loadLegacy` cached), and only after the master lock is re-read and still shows this
   * instance's own last write. Between the fencing and the delete a later owner (a granular
   * deployment, say) can open the partition and take a lock over: the master check refuses the
   * whole purge if it opened first, and the token refuses the delete of any lock it rewrote after.
   * A lock refused on version is someone else's now and counts as resolved; a lock `loadLegacy`
   * found missing is not deleted. The sweep marker is deleted by path. `legacyResolved` is only set
   * once the master check passes and no delete failed, so a failure simply retries the whole set
   * on the next `open()` (re-fencing an already-fenced lock is a harmless no-op bump).
   */
  private def purgeLegacyLocks(
    topicPartition:  TopicPartition,
    bucketAndPrefix: CloudLocation,
    lockKeys:        List[String],
  ): Unit = {
    val bucket = bucketAndPrefix.bucket
    val lockDeletes = lockKeys.flatMap { pk =>
      gcGet(topicPartition, pk).map(entry =>
        ConditionalDelete(generateGranularLockFilePath(connectorTaskId, topicPartition, pk, directoryFileName),
                          entry.eTag,
        ),
      )
    }
    val markerPath = generateSweepMarkerPath(connectorTaskId, topicPartition, directoryFileName)
    if (!stillOwnsMaster(bucket, topicPartition)) {
      logger.warn(
        s"[${connectorTaskId.show}] Legacy-lock purge for $topicPartition skipped: the master lock no longer " +
          "shows this task's last write; will retry on the next open().",
      )
    } else {
      val outcomes = lockDeletes.grouped(gcBatchSize).flatMap { chunk =>
        chunk.zip(storageInterface.deleteFilesIfMatch(bucket, chunk).padTo(chunk.size, MissingDeleteOutcome))
      }.toList
      val failed  = outcomes.collect { case (d, DeleteOutcome.Failed(err)) => d.path -> err.message() }
      val changed = outcomes.collect { case (d, DeleteOutcome.PreconditionFailed) => d.path }
      if (changed.nonEmpty) {
        logger.info(
          s"[${connectorTaskId.show}] Legacy-lock purge for $topicPartition left ${changed.size} lock(s) in place: " +
            s"rewritten since they were fenced (${changed.mkString(", ")})",
        )
      }
      val markerDeleted = storageInterface.deleteFiles(bucket, Seq(markerPath)) match {
        case Right(_) => true
        case Left(err) =>
          logger.warn(
            s"[${connectorTaskId.show}] Legacy-lock purge for $topicPartition could not delete the sweep marker; " +
              s"will retry on the next open(): ${err.message()}",
          )
          false
      }
      failed.headOption.foreach {
        case (path, reason) =>
          logger.warn(
            s"[${connectorTaskId.show}] Legacy-lock purge for $topicPartition failed for ${failed.size} lock(s) " +
              s"(first: $path: $reason); will retry on the next open()",
          )
      }
      if (failed.isEmpty && markerDeleted) {
        val _ = legacyResolved.put(topicPartition, true)
        gcRemoveAllForTp(topicPartition)
        metrics.incrementLegacyLocksPurged()
        logger.info(
          s"[${connectorTaskId.show}] Purged ${outcomes.size - changed.size} legacy granular lock(s) for $topicPartition.",
        )
      }
    }
  }

  /**
   * Applies the ownership-epoch read rule to a legacy snapshot read before anything is cached:
   * `Left` when the lock was written under a newer epoch (this instance is fenced), otherwise the
   * read itself, unchanged.
   */
  private def fencedOnRead[A](
    topicPartition: TopicPartition,
    partitionKey:   String,
    read:           Either[A, ObjectWithETag[IndexFile]],
  ): Either[SinkError, Either[A, ObjectWithETag[IndexFile]]] =
    read match {
      case Right(owe) => rejectIfFenced(topicPartition, partitionKey, owe.wrappedObject).map(_ => read)
      case Left(_)    => read.asRight
    }

  /**
   * Loads (fences) one legacy granular lock: resolves an in-flight `PendingState` or bumps a
   * clean lock's eTag, taking ownership of a poison blob if found. On a lost race resolving a
   * pending chain (a live granular zombie also driving it), re-reads and retries up to
   * [[MaxLegacyBumpAttempts]] times before giving up for this cycle -- the caller
   * (`snapshotLegacyLocks`) treats a final failure as non-fatal and retries on the next `open()`.
   */
  private def loadLegacy(topicPartition: TopicPartition, partitionKey: String): Either[SinkError, Option[Offset]] =
    // No held epoch: not ours, touch nothing (see `rejectNewerEpoch`).
    heldEpoch(topicPartition).flatMap(_ => bucketAndPrefixFn(topicPartition)).flatMap { bp =>
      val path = generateGranularLockFilePath(connectorTaskId, topicPartition, partitionKey, directoryFileName)

      def attempt(attemptsLeft: Int): Either[SinkError, Option[Offset]] =
        fencedOnRead(topicPartition, partitionKey, tryOpen(bp.bucket, path)) match {
          case Left(fenced)                                                                           => fenced.asLeft
          case Right(Right(owe @ ObjectWithETag(IndexFile(_, committedOffset, Some(pending), _), _))) =>
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

          case Right(Right(owe @ ObjectWithETag(IndexFile(_, committedOffset, None, _), _))) =>
            gcPut(topicPartition, partitionKey, GranularCacheEntry(committedOffset, owe.eTag))
            bumpLegacyLock(topicPartition, partitionKey, committedOffset, MaxLegacyBumpAttempts)

          case Right(Left(_: FileNotFoundError)) =>
            Option.empty[Offset].asRight

          case Right(Left(EmptyFileError(_, eTag))) =>
            // Take ownership exactly as ensureGranularLock does.
            heldEpoch(topicPartition).flatMap { epoch =>
              storageInterface.writeBlobToFile(
                bp.bucket,
                path,
                ObjectWithETag(IndexFile(lockOwner, Option.empty, Option.empty, Some(epoch)), eTag),
              ).bimap(
                err => NonFatalCloudSinkError.unswallowable(err.message(), err.toExceptionOption): SinkError,
                owe => {
                  gcPut(topicPartition, partitionKey, GranularCacheEntry(None, owe.eTag))
                  Option.empty[Offset]
                },
              )
            }

          case Right(Left(err)) =>
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
          fencedOnRead(topicPartition, partitionKey, tryOpen(bp.bucket, path)) match {
            case Left(fenced) => fenced.asLeft
            case Right(Right(owe @ ObjectWithETag(IndexFile(_, co2, None, _), _))) =>
              gcPut(topicPartition, partitionKey, GranularCacheEntry(co2, owe.eTag))
              bumpLegacyLock(topicPartition, partitionKey, co2, attemptsLeft - 1)
            case Right(Right(owe @ ObjectWithETag(IndexFile(_, co2, Some(pending), _), _))) =>
              gcPut(topicPartition, partitionKey, GranularCacheEntry(None, owe.eTag))
              pendingOperationsProcessors.processPendingOperations(
                topicPartition,
                co2,
                pending,
                (tp, co, ps) => updateForPartitionKey(tp, partitionKey, co, ps),
              )
            case Right(Left(rerr)) =>
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
    refuseIfFenced(topicPartition, partitionKey).flatMap { _ =>
      gcGet(topicPartition, partitionKey) match {
        case Some(cached) =>
          metrics.incrementGranularCacheHits()
          cached.offset.asRight
        case None =>
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
    // Deterministic path derived from connector task ID, topic-partition, and partition key
    val path = generateGranularLockFilePath(connectorTaskId, topicPartition, partitionKey, directoryFileName)
    for {
      // No held epoch: not ours, touch nothing -- no read, no cache entry, no pending chain.
      _               <- heldEpoch(topicPartition)
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      result <- tryOpen(bucketAndPrefix.bucket, path) match {
        // Lock exists: take it over if an older owner wrote it, resolve any PendingState, cache.
        case Right(objectWithEtag) =>
          resolveAndCacheGranularLock(topicPartition, partitionKey, objectWithEtag)

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
      eTag  <- resolveGranularETag(topicPartition, partitionKey)
      epoch <- heldEpoch(topicPartition)
      index = ObjectWithETag(
        IndexFile(lockOwner, committedOffset, pendingState, Some(epoch)),
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
   * Caches a granular lock read from storage, resolving any PendingState first, and returns the
   * resolved committed offset. Used by every path that loads an existing lock (`loadGranularLock`
   * and `ensureGranularLock`, including its lost-race re-reads).
   *
   * The ownership-epoch read rule runs first (a newer epoch fences this instance). A lock written
   * under an older epoch is then taken over (`takeOverGranularLock`) before anything is cached or
   * driven, so every token an older instance still holds for the key stops being valid.
   *
   * When PendingState is present the pending upload/copy/delete operations are completed (or rolled
   * back) via processPendingOperations before the resolved offset is cached. On failure the cache
   * entry is removed so a later access retries cleanly from storage.
   */
  private def resolveAndCacheGranularLock(
    topicPartition: TopicPartition,
    partitionKey:   String,
    read:           ObjectWithETag[IndexFile],
  ): Either[SinkError, Option[Offset]] =
    rejectIfFenced(topicPartition, partitionKey, read.wrappedObject)
      .flatMap(_ => takeOverGranularLock(topicPartition, partitionKey, read))
      .flatMap {
        // Crash recovery: an earlier commit stopped mid-chain; resolve pending ops before caching
        case owe @ ObjectWithETag(IndexFile(_, committedOffset, Some(pendingState), _), _) =>
          logger.info(s"Loaded granular lock with PendingState for $topicPartition/$partitionKey, resolving")
          // Seed cache with offset=None and the current eTag so processPendingOperations can
          // perform conditional writes; the resolved offset replaces None on success
          gcPut(topicPartition, partitionKey, GranularCacheEntry(None, owe.eTag))
          val fnUpdate: (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]] =
            (tp, co, ps) => updateForPartitionKey(tp, partitionKey, co, ps)
          pendingOperationsProcessors.processPendingOperations(
            topicPartition,
            committedOffset,
            pendingState,
            fnUpdate,
          ).left.map { err =>
            gcRemove(topicPartition, partitionKey)
            err
          }

        // Clean lock: no crash recovery needed, cache offset and eTag directly
        case owe @ ObjectWithETag(IndexFile(_, committedOffset, None, _), _) =>
          logger.info(s"Loaded granular lock for $topicPartition/$partitionKey, offset=$committedOffset")
          gcPut(topicPartition, partitionKey, GranularCacheEntry(committedOffset, owe.eTag))
          committedOffset.asRight
      }

  /**
   * Takes over a granular lock written under an older ownership epoch than the one this instance
   * holds: rewrites it with this instance's owner and epoch, committed offset and pending state
   * unchanged, conditional on the token just read. Loading a key is the first thing a new owner
   * does before it routes a record to it, so from then on every token an older instance holds for
   * the key is invalid (its next CAS fails), and a reload by that instance reads the newer epoch
   * and is fenced by the read rule. Without this a zombie that still holds the token it used
   * before the rebalance would win its next commit on the key and the legitimate owner would fail.
   *
   * A lock already at the held epoch and owned by this instance was written by it (an idle eviction
   * and reload) and is returned unchanged, with no write. A lock at the held epoch with another
   * owner is taken over like an older one (`heldByUsOrNewer`). Must run after `rejectIfFenced`, so a newer epoch never
   * reaches it. A lost write race is re-read and decided again (`takeOverAttempt`). Returns the
   * version to cache and drive from: the object written, under the token the write returned, or,
   * when only the write's response was lost, the object as re-read.
   */
  private def takeOverGranularLock(
    topicPartition: TopicPartition,
    partitionKey:   String,
    read:           ObjectWithETag[IndexFile],
  ): Either[SinkError, ObjectWithETag[IndexFile]] =
    for {
      mine  <- heldEpoch(topicPartition)
      bp    <- bucketAndPrefixFn(topicPartition)
      path   = generateGranularLockFilePath(connectorTaskId, topicPartition, partitionKey, directoryFileName)
      taken <- takeOverAttempt(topicPartition, partitionKey, bp.bucket, path, mine, read, MaxLegacyBumpAttempts)
    } yield taken

  /**
   * One take-over write of `read`, and on failure a re-read that applies the epoch rule again to
   * whatever is stored: a failed write means another instance wrote the key between our read and
   * our write, that our own write landed with its response lost, or a transient write error.
   *  - a newer epoch fences this instance (never re-taken over), which is what keeps a paused stale
   *    loader from overwriting the newer owner that took the key over meanwhile;
   *  - our own epoch and owner is our landed write: it is used as stored, with no second write, and
   *    counts as a take-over;
   *  - an older epoch, or our epoch under another owner (a zombie commit, or a transient failure),
   *    is taken over again while writes remain. Once they are spent it is a plain retriable error,
   *    not a fatal one: nothing shows this instance was replaced, and a run of transient write
   *    errors must not need a task restart, so RETRY re-delivers the record and the next load takes
   *    the key over afresh.
   * At most `attemptsLeft` writes, and every failed one, the last included, is re-read, so a final
   * write that landed with its response lost is adopted rather than failed. A re-read that fails
   * (missing key, 0-byte blob, transient read error) is an unswallowable non-fatal error, as
   * ensureGranularLock's own re-reads are. No failure caches anything or proceeds with a token
   * this instance has not claimed.
   */
  private def takeOverAttempt(
    topicPartition: TopicPartition,
    partitionKey:   String,
    bucket:         String,
    path:           String,
    mine:           Long,
    read:           ObjectWithETag[IndexFile],
    attemptsLeft:   Int,
  ): Either[SinkError, ObjectWithETag[IndexFile]] =
    if (heldByUsOrNewer(read.wrappedObject, mine)) read.asRight
    else {
      val bump = read.wrappedObject.copy(owner = lockOwner, epoch = Some(mine))
      storageInterface.writeBlobToFile(bucket, path, ObjectWithETag(bump, read.eTag)) match {
        case Right(bumped) =>
          metrics.incrementGranularLockOwnershipBumps()
          logger.debug(
            s"[${connectorTaskId.show}] Took over granular lock $topicPartition/$partitionKey " +
              s"(epoch ${read.wrappedObject.epoch.getOrElse(0L)} -> $mine)",
          )
          // What we wrote, under the new token: never depend on how a backend echoes the object.
          ObjectWithETag(bump, bumped.eTag).asRight
        case Left(err) =>
          metrics.incrementGranularLockOwnershipBumpRaces()
          logger.info(
            s"[${connectorTaskId.show}] Lost the race taking over granular lock $topicPartition/$partitionKey " +
              s"(${err.message()}); re-reading (writes left: ${attemptsLeft - 1})",
          )
          tryOpen(bucket, path) match {
            case Right(reread) =>
              rejectIfFenced(topicPartition, partitionKey, reread.wrappedObject).flatMap { _ =>
                if (heldByUsOrNewer(reread.wrappedObject, mine)) {
                  // Our own write landed and only its response was lost.
                  metrics.incrementGranularLockOwnershipBumps()
                  logger.debug(
                    s"[${connectorTaskId.show}] Confirmed take-over of granular lock $topicPartition/$partitionKey " +
                      s"on re-read (epoch $mine)",
                  )
                  reread.asRight
                } else if (attemptsLeft > 1)
                  takeOverAttempt(topicPartition, partitionKey, bucket, path, mine, reread, attemptsLeft - 1)
                else {
                  // Not fatal: the key is still held under an older epoch (or ours under another
                  // owner), so this instance has not been fenced, and the writes may simply have
                  // failed transiently. RETRY re-delivers the record and the next load starts a fresh
                  // take-over.
                  val error = NonFatalCloudSinkError(
                    s"Exhausted take-over attempts for granular lock $topicPartition/$partitionKey: ${err.message()}",
                    err.toExceptionOption,
                  )
                  logger.error(error.message)
                  error.asLeft
                }
              }
            case Left(readErr) =>
              NonFatalCloudSinkError.unswallowable(
                s"Failed to re-read granular lock $topicPartition/$partitionKey after a lost take-over race: " +
                  readErr.message(),
                readErr.toExceptionOption,
              ).asLeft
          }
      }
    }

  /**
   * Whether a granular lock needs no take-over by an instance holding `mine`: written under a newer
   * epoch (which `rejectIfFenced` has already refused), or under `mine` by this instance itself. An
   * equal epoch is not enough: with a missing or 0-byte master lock two instances both take the
   * clock-only epoch, so a lock at `mine` with another owner was written by an instance that may
   * still hold its token, and is taken over like an older one.
   */
  private def heldByUsOrNewer(lock: IndexFile, mine: Long): Boolean = {
    val epoch = lock.epoch.getOrElse(0L)
    epoch > mine || (epoch == mine && lock.owner == lockOwner)
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
    // A fenced partition serves no key, cached or not.
    if (isFenced(topicPartition)) refuseIfFenced(topicPartition, partitionKey)
    // Fast path: cache hit means the lock file already exists and its eTag is tracked
    else if (gcContainsKey(topicPartition, partitionKey)) {
      metrics.incrementGranularCacheHits()
      ().asRight
    } else {
      metrics.incrementGranularCacheMisses()
      val path = generateGranularLockFilePath(connectorTaskId, topicPartition, partitionKey, directoryFileName)
      for {
        bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
        epoch           <- heldEpoch(topicPartition)
        _ <- tryOpen(bucketAndPrefix.bucket, path) match {
          // Lock exists in storage: resolve any PendingState and cache the result
          case Right(objectWithEtag) =>
            resolveAndCacheGranularLock(topicPartition, partitionKey, objectWithEtag).void

          // Lock does not exist yet: create an empty one with NoOverwrite precondition
          case Left(_: FileNotFoundError) =>
            val idx = IndexFile(lockOwner, None, None, Some(epoch))
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
                  resolveAndCacheGranularLock(topicPartition, partitionKey, existing).void
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
            val idx = IndexFile(lockOwner, None, None, Some(epoch))
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
                  resolveAndCacheGranularLock(topicPartition, partitionKey, existing).void
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

  /**
   * Releases the partitions Kafka Connect is closing (see `IndexManager.closePartitions`). They
   * leave `ownedPartitions` first, so a batch temp sweep -- even one already mid-pass -- stops at
   * its next delete chunk for them. The GC deletes queued for them are then dropped, not drained:
   * the partition is no longer this task's, and the drain discards a revoked partition's items
   * anyway. Obsolete locks dropped here stay in storage for the orphan sweep. Every other piece of
   * partition state is then cleared: seeked offset, master eTag, held epoch, granular cache, fence
   * and legacy-resolution flag. The release runs even if dropping the queue throws. A drain or
   * sweep run already in flight can still re-queue items afterwards, or delete one it already
   * polled; every such delete is conditional on the version GC selected, so a lock rewritten since
   * (by a new owner, or by this task reclaiming the key) is not deleted.
   */
  override def closePartitions(topicPartitions: Set[TopicPartition]): Unit =
    if (topicPartitions.nonEmpty) {
      topicPartitions.foreach(ownedPartitions.remove)
      try dropQueuedGcItems(topicPartitions)
      finally {
        val failures = topicPartitions.toList.flatMap(tp => Try(releasePartition(tp)).failed.toOption)
        logger.debug(s"[${connectorTaskId.show}] Released ${topicPartitions.size} closed partition(s)")
        resumeIfStillOwning()
        failures.headOption.foreach(throw _)
      }
    }

  /**
   * Re-opens the background-work gate `CloudSinkTask.close` suspended, when this instance still
   * holds an ownership epoch for some partition. A cooperative revoke that adds nothing calls
   * `close(revoked)` only -- Connect skips `open()` -- so without this the GC drain and sweeps of
   * the retained partitions would stay off until the next rebalance. After a close of every held
   * partition (an eager rebalance, or before `stop()`) the gate stays shut until `open()`.
   */
  private def resumeIfStillOwning(): Unit =
    if (executorsStarted && tpEpoch.nonEmpty) {
      acceptingWork = true
      logger.debug(s"[${connectorTaskId.show}] Background work resumed for the partitions still held")
    }

  /** Removes the GC items queued for `tps` without deleting anything; counted as revoked skips. */
  private def dropQueuedGcItems(tps: Set[TopicPartition]): Unit = {
    val dropped = new java.util.concurrent.atomic.AtomicLong(0L)
    val _ = gcQueue.removeIf { item =>
      val closing = tps.contains(item.topicPartition)
      if (closing) { val _ = dropped.incrementAndGet() }
      closing
    }
    metrics.setGcQueueDepth(gcQueue.size())
    if (dropped.get() > 0) {
      metrics.incrementGcLocksSkippedRevoked(dropped.get())
      logger.info(
        s"[${connectorTaskId.show}] Dropped ${dropped.get()} queued GC delete(s) for closed partition(s) " +
          s"${tps.mkString(", ")}; their obsolete locks are left for the orphan sweep.",
      )
    }
  }

  /** Drops every piece of in-memory state held for `tp`: granular cache and partition state. */
  private def releasePartition(tp: TopicPartition): Unit = {
    evictAllGranularLocks(tp)
    clearTopicPartitionState(tp)
  }

  /** Removes this partition's seeked offset, eTag, held epoch, fence and queued GC items, so background threads stop operating on it. */
  override def clearTopicPartitionState(topicPartition: TopicPartition): Unit = {
    // Revoked or failed open: the batch temp sweep must stop deleting under it (LC-349).
    val _ = ownedPartitions.remove(topicPartition)
    seekedOffsets.remove(topicPartition)
    val _ = topicPartitionToETags.remove(topicPartition)
    val _ = tpEpoch.remove(topicPartition)
    val _ = sweepCursor.remove(topicPartition)
    // A revoked partition is no longer this task's to fail on; a re-open decides ownership afresh.
    // A revoked partition's queued items would be discarded by the drain anyway; dropping them
    // here keeps the queued ones from surviving into a later re-assignment. A drain or sweep run
    // already in flight can still re-queue items afterwards; their deletes are conditional on the
    // version selected, so a lock rewritten since survives them.
    dropFence(topicPartition, dropQueue = true, below = None)
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
   * tasks (see architecture doc). After a failure the caller runs `confirmMasterFenced`,
   * which is the only place that may move the cached eTag to a re-read version, and only
   * when that version is provably this instance's own write.
   */
  // TEST-ONLY SEAM — DO NOT WIRE FROM PRODUCTION CODE.
  // No-op by default. Fires inside `updateMasterLock`, between the cached-eTag read and the
  // eTag-conditional storage write, to engineer a deterministic zombie rebalance race in tests.
  // See `IndexManagerV2TestHooks` for the install/clear API.
  @volatile private[seek] var testHookPreWriteMasterLockBarrier: () => Unit = () => ()

  // TEST-ONLY SEAM — DO NOT WIRE FROM PRODUCTION CODE.
  // No-op by default. Fires inside `ownershipTaken`, after the new epoch is published and before
  // the previous episode's fences are cleared, so a test can fence against the new epoch there.
  // See `IndexManagerV2TestHooks` for the install/clear API.
  @volatile private[seek] var testHookOwnershipPublished: (TopicPartition, Long) => Unit = (_, _) => ()

  /**
   * After a failed master-lock write: re-read the lock and decide whether a newer owner holds it.
   * Since our open() bump, any write that lands on the master lock is either ours or another
   * instance's ownership bump (a previous owner's floor write would fail its CAS against our bump).
   * The re-read has four outcomes:
   *
   *  - a different owner, or a higher epoch: this instance has been replaced; the partition is
   *    marked fenced and `true` is returned.
   *  - our owner and our epoch, no pending state, but an eTag other than the cached one: the
   *    failed write landed and only its response was lost (e.g. a timeout whose SDK retry then
   *    failed its precondition against the object the first attempt had already changed).
   *    `lockOwner` is unique to this instance and the epoch was assigned by this instance's own
   *    open(), so the stored version can only be our own write; it is adopted (cached eTag, and the
   *    seeked offset moved forward to its committed offset, never back) so the next cycle's write
   *    succeeds. Without this every later write would fail its CAS against the stale cached eTag
   *    and the partition would never return an offset again.
   *  - our owner and our epoch with the cached eTag, a lower epoch (not expected), or any other
   *    shape of our own lock: transient; nothing is changed and the next cycle retries.
   *  - the lock can't be read: transient, likewise.
   *
   * Runs on the task thread (from preCommit), as `updateMasterLock` does. In the modes that
   * write the master lock through `updateMasterLock` (granular PARTITIONBY), nothing else writes
   * the master lock or its cached eTag while the partition is owned, so the adopt cannot race
   * another writer of that cache entry.
   */
  override def confirmMasterFenced(topicPartition: TopicPartition): Boolean =
    (for {
      mine <- tpEpoch.get(topicPartition)
      loc  <- bucketAndPrefixFn(topicPartition).toOption
      lock <- tryOpen(loc.bucket, generateLockFilePath(connectorTaskId, topicPartition, directoryFileName)).toOption
    } yield (lock, mine)) match {
      case Some((ObjectWithETag(lock, _), mine)) if lock.owner != lockOwner || lock.epoch.getOrElse(0L) > mine =>
        val _ = markFenced(
          topicPartition,
          mine,
          s"master lock owner ${lock.owner} epoch ${lock.epoch.getOrElse(0L)}, held epoch $mine",
        )
        logger.error(
          s"[${connectorTaskId.show}] Fenced on $topicPartition: the master lock now belongs to another instance " +
            s"(owner ${lock.owner}, epoch ${lock.epoch.getOrElse(0L)}; this instance's lockUuid $lockOwner, " +
            s"epoch $mine). " +
            s"The task will fail on its next put() so a restart can re-take ownership.",
        )
        true
      case Some((stored @ ObjectWithETag(lock, storedETag), mine))
          if lock.epoch.contains(mine) && lock.pendingState.isEmpty &&
            !topicPartitionToETags.get(topicPartition).contains(storedETag) =>
        adoptOwnMasterWrite(topicPartition, stored)
        false
      case _ => false
    }

  /** Caches a master-lock version this instance provably wrote itself; see `confirmMasterFenced`. */
  private def adoptOwnMasterWrite(topicPartition: TopicPartition, stored: ObjectWithETag[IndexFile]): Unit = {
    topicPartitionToETags.put(topicPartition, stored.eTag)
    stored.wrappedObject.committedOffset.foreach { o =>
      if (seekedOffsets.get(topicPartition).forall(_.value < o.value)) seekedOffsets.put(topicPartition, o)
    }
    logger.warn(
      s"[${connectorTaskId.show}] Master lock write for $topicPartition landed but its response was lost: the " +
        s"stored lock carries this instance's lockUuid and epoch with a new version, so that version was adopted " +
        s"(committedOffset ${stored.wrappedObject.committedOffset.map(_.value)}). The next write proceeds from it.",
    )
  }

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
      epoch <- heldEpoch(topicPartition)
      index = ObjectWithETag(
        IndexFile(lockOwner, committedOffset, None, Some(epoch)),
        eTag,
      )
      _ = testHookPreWriteMasterLockBarrier() // test seam — no-op in production
      blobFileWrite <- storageInterface.writeBlobToFile(
        bucketAndPrefix.bucket,
        path,
        index,
      ) match {
        case Left(err: UploadError) =>
          // Do NOT blindly re-read the master lock to refresh the cached eTag here: adopting
          // whatever is stored would hand a zombie the newer owner's fencing token. The caller
          // runs `confirmMasterFenced`, which re-reads and decides: a newer owner fences the
          // partition; our own write whose response was lost is adopted so the next cycle
          // succeeds; anything else is transient and the unchanged eTag is retried next cycle.
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
        // Each key is taken with the token of the cache entry that selected it: the version of the
        // lock this instance last loaded, took over or wrote. The delete is conditional on it, so a
        // lock rewritten after this point (by a newer owner's take-over, or by this instance
        // reclaiming the key) is not deleted.
        val keysToRemove = inner.entrySet().asScala.collect {
          case entry
              if (entry.getValue.offset.isEmpty ||
                entry.getValue.offset.exists(_.value < globalSafeOffset.value - 1L)) &&
                !activePartitionKeys.contains(entry.getKey) =>
            entry.getKey -> entry.getValue.eTag
        }.toList

        if (keysToRemove.isEmpty) ().asRight
        else
          bucketAndPrefixFn(topicPartition).map { bucketAndPrefix =>
            // Evict from cache and enqueue for async deletion; no cloud I/O here
            keysToRemove.foreach {
              case (pk, eTag) =>
                gcRemove(topicPartition, pk)
                val path = generateGranularLockFilePath(connectorTaskId, topicPartition, pk, directoryFileName)
                gcQueue.add(GcItem(bucketAndPrefix.bucket, path, topicPartition, pk, eTag = Some(eTag)))
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
   *  2. Group eligible items by bucket and chunk them. `.tmp` orphans go in chunks of
   *     `gcBatchSize`. `.lock` items go in chunks of `gcBatchSize` only on a store whose conditional
   *     multi-object delete is one request (`conditionalDeleteIsBatched`: S3 with batch delete on);
   *     on a store that deletes one object per request (GCS, ADLS, S3 with batch delete off) each
   *     `.lock` chunk holds one item, so every storage call is preceded by its own re-check.
   *  3. Delete each chunk. Right before each `.lock` chunk is sent, the reclaim filter runs again
   *     over that chunk, so a key the `put()` thread reloaded while earlier deletes were in flight
   *     is kept. A reload that lands during the one delete call covering its key can still lose its
   *     lock: the key's next commit then fails its lock write before anything reaches a final path,
   *     and the restarted task replays from the master floor. A `.lock` is deleted only while it is at
   *     the version GC selected (`deleteFilesIfMatch` with the item's token): a lock another
   *     writer rewrote since is left in place and the item dropped. That is what keeps a delete
   *     selected before a handover (or before this instance reclaimed the key, which the reclaim
   *     filter cannot see once the cache is evicted, as at `stop()`) from removing a live lock and
   *     its key's dedup floor. `.lock.tmp.<uuid>` orphans are deleted by path (`deleteFiles`). On
   *     a failure, re-enqueue items up to `MaxGcRetries` times. Failures are logged but never
   *     propagated.
   */
  private[seek] def drainGcQueue(): Unit =
    drainGcItems(Iterator.continually(gcQueue.poll()).takeWhile(_ != null))

  /** Phases 1-3 of [[drainGcQueue]] for items already taken off the queue. */
  private def drainGcItems(polled: Iterator[GcItem]): Unit = {
    // Phase 1: apply partition-revoked and key-reclaimed filters
    val eligible = polled.filter { item =>
      if (!seekedOffsets.contains(item.topicPartition)) {
        logger.debug(
          s"GC discarding ${item.topicPartition}/${item.partitionKey}: partition no longer owned by this task",
        )
        metrics.incrementGcLocksSkippedRevoked()
        false
      } else if (isFenced(item.topicPartition)) {
        // A newer owner holds the partition: its locks are not ours to delete. The delete would be
        // refused anyway for a lock the newer owner rewrote; this skips the requests. Counted with
        // the revoked skips.
        logger.debug(s"GC discarding ${item.topicPartition}/${item.partitionKey}: this instance is fenced")
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
            val (locks, tmpOrphans) = items.partition(_.kind == GcKind.Lock)
            tmpOrphans.grouped(gcBatchSize).foreach { chunk =>
              storageInterface.deleteFiles(bucket, chunk.map(_.path)) match {
                case Left(err) =>
                  retryFailedDeletes(bucket, chunk, err.message())
                case Right(_) =>
                  metrics.incrementGcLocksDeleted(chunk.size.toLong)
                  logger.debug(s"Background GC deleted ${chunk.size} orphaned .tmp file(s) from bucket=$bucket")
              }
              unprocessed --= chunk
            }
            // A `.lock` item always carries the token it was selected at; one without is never
            // deleted by path.
            val (conditional, untokened) = locks.partition(_.eTag.isDefined)
            if (untokened.nonEmpty) {
              metrics.incrementGcLocksSkippedChanged(untokened.size.toLong)
              unprocessed --= untokened
            }
            // A store that deletes one object per request gets one item per call, so the re-check
            // below runs right before each storage call, not once ahead of a run of them.
            val lockChunkSize = if (storageInterface.conditionalDeleteIsBatched) gcBatchSize else 1
            conditional.grouped(lockChunkSize).foreach { selected =>
              // Re-check the reclaim filter right before each chunk: the put() thread may have
              // reloaded a key while earlier chunks were being deleted. A reload reads the lock at
              // the version GC selected, so the token alone would not stop this delete.
              val (reclaimed, chunk) = selected.partition(i => gcContainsKey(i.topicPartition, i.partitionKey))
              if (reclaimed.nonEmpty) {
                logger.debug(
                  s"GC skipping ${reclaimed.map(i => s"${i.topicPartition}/${i.partitionKey}").mkString(", ")}: " +
                    "reclaimed by new writer",
                )
                metrics.incrementGcLocksSkippedReclaimed(reclaimed.size.toLong)
                unprocessed --= reclaimed
              }
              val outcomes =
                if (chunk.isEmpty) Vector.empty
                else storageInterface.deleteFilesIfMatch(
                  bucket,
                  chunk.map(i => ConditionalDelete(i.path, i.eTag.getOrElse(""))),
                ).toVector
              // One outcome per item by contract; an item left without one is treated as failed.
              val results = chunk.zipWithIndex.map {
                case (item, n) =>
                  item -> outcomes.lift(n).getOrElse(
                    DeleteOutcome.Failed(
                      FileDeleteError(new IllegalStateException("no outcome reported for the delete"), item.path),
                    ),
                  )
              }
              val deleted = results.collect { case (i, DeleteOutcome.Deleted | DeleteOutcome.NotFound) => i }
              val changed = results.collect { case (i, DeleteOutcome.PreconditionFailed) => i }
              val failed  = results.collect { case (i, DeleteOutcome.Failed(err)) => i -> err.message() }
              if (deleted.nonEmpty) {
                metrics.incrementGcLocksDeleted(deleted.size.toLong)
                logger.debug(s"Background GC deleted ${deleted.size} lock file(s) from bucket=$bucket")
              }
              if (changed.nonEmpty) {
                metrics.incrementGcLocksSkippedChanged(changed.size.toLong)
                logger.debug(
                  s"Background GC left ${changed.size} lock file(s) in place in bucket=$bucket: rewritten since " +
                    s"they were selected (${changed.map(i => s"${i.topicPartition}/${i.partitionKey}").mkString(", ")})",
                )
              }
              if (failed.nonEmpty) retryFailedDeletes(bucket, failed.map(_._1), failed.head._2)
              unprocessed --= chunk
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
   * Re-enqueues GC items whose delete failed (nothing is known about them), up to `MaxGcRetries`
   * attempts each. Deleting again is safe: a `.lock` delete stays conditional on the item's token.
   */
  private def retryFailedDeletes(bucket: String, failed: Seq[GcItem], reason: String): Unit = {
    metrics.incrementGcDeleteFailures()
    logger.warn(
      s"Background GC delete failed for ${failed.size} file(s) from bucket=$bucket: $reason. " +
        s"Re-enqueuing conservatively (some items may have been deleted -- idempotent retry is safe).",
    )
    val retryable = failed.filter(_.retryCount < MaxGcRetries)
    retryable.foreach(i => gcQueue.add(i.copy(retryCount = i.retryCount + 1)))
    if (retryable.nonEmpty) {
      metrics.incrementGcDeleteRetries(retryable.size.toLong)
      logger.debug(
        s"Re-enqueued ${retryable.size} item(s) for retry (dropped ${failed.size - retryable.size} that exceeded max retries)",
      )
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
        //
        // A partition is visited only while the budget still covers one lock read plus the master
        // re-check. With less, it would get its marker written (and count as scanned) while reading
        // nothing, and look swept for a whole interval; it is left for the next cycle instead.
        for (
          tp <- Random.shuffle(seekedOffsets.keys.toList)
          if readsRemaining >= IndexManagerV2.MinGcSweepMaxReads && !isFenced(tp)
        ) {
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

  // Per partition: the last lock path the orphan sweep read, where the next pass starts (see
  // `sweepPartition`). In memory only: lost on a restart or a close, which only means the next pass
  // starts from the beginning of the listing. Written and read by the sweep thread; cleared with
  // the rest of the partition state.
  private val sweepCursor = TrieMap.empty[TopicPartition, String]

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
      // Classify each .lock file and GET-read only those that pass the filter chain, respecting the
      // budget. One read is kept back for the master re-check below. Once a read fences the
      // partition, nothing more of it is read. The pass starts after the last lock the previous
      // pass read and wraps around, so locks retained at the start of the listing cannot use up
      // every pass's budget and starve the ones listed after them.
      val lockReadBudget = readsRemaining - 1
      val (afterCursor, upToCursor) =
        sweepCursor.get(tp).fold((files, Seq.empty[FileMetadata]))(last => files.partition(_.file > last))
      val (selected, lockReads, lastRead) =
        (afterCursor ++ upToCursor).foldLeft((Vector.empty[GcItem], 0, Option.empty[String])) {
          case (acc @ (_, readsUsed, _), _) if readsUsed >= lockReadBudget || isFenced(tp) => acc
          case (acc @ (picked, readsUsed, _), fileMeta) =>
            classifyLockFile(tp, fileMeta, ageThreshold, prefix) match {
              case SweepSkip => acc
              case SweepNeedsRead(partitionKey) =>
                (picked ++ selectObsoleteLock(bucket, fileMeta.file, tp, partitionKey, masterOffset),
                 readsUsed + 1,
                 Some(fileMeta.file),
                )
            }
        }
      lastRead.foreach(sweepCursor.put(tp, _))
      if (selected.isEmpty) (0, lockReads)
      else if (stillOwnsMaster(bucket, tp)) {
        selected.foreach(gcQueue.add)
        (selected.size, lockReads + 1)
      } else {
        metrics.incrementSweepSkippedOwnershipLost()
        logger.info(
          s"[${connectorTaskId.show}] Sweep: ${selected.size} obsolete lock(s) of $tp not enqueued: the master " +
            "lock no longer shows this task's last write (the partition was taken over, or it could not be read)",
        )
        (0, lockReads + 1)
      }
    }
    result.getOrElse((0, 0))
  }

  /**
   * The ownership re-check GC makes before it deletes anything it selected (the orphan sweep, after
   * its lock reads and before it enqueues; the legacy purge, before its deletes):
   * the master lock still carries the token of this instance's own last master write. A new owner's
   * `open()` always rewrites the master (its owner, a new epoch), so a match shows no other instance
   * opened the partition before this read, hence none has taken over a lock the sweep read: any such
   * take-over comes later and changes the lock's token, and the conditional delete then fails. That
   * covers the case the epoch read rule cannot: a lock another instance holds at the SAME epoch (two
   * clock-only epochs over a missing master). A mismatch is not proof of a takeover (this
   * instance's own commit may have just moved the master), so it only skips this cycle; a master
   * under a higher epoch than the one held does prove it, and fences the partition.
   */
  private def stillOwnsMaster(bucket: String, tp: TopicPartition): Boolean = {
    val heldAtRead = tpEpoch.get(tp)
    tryOpen(bucket, generateLockFilePath(connectorTaskId, tp, directoryFileName)) match {
      case Right(master) =>
        if (topicPartitionToETags.get(tp).contains(master.eTag) && !isFenced(tp)) true
        else {
          heldAtRead.foreach { mine =>
            val theirs = master.wrappedObject.epoch.getOrElse(0L)
            if (theirs > mine && markFenced(tp, mine, s"master lock epoch $theirs > held epoch $mine")) {
              logger.error(
                s"[${connectorTaskId.show}] Fenced on $tp: a GC master re-check read a master lock under epoch " +
                  s"$theirs (owner ${master.wrappedObject.owner}) > this instance's epoch $mine (lockUuid $lockOwner).",
              )
            }
          }
          false
        }
      case Left(err) =>
        logger.warn(s"[${connectorTaskId.show}] GC: master lock re-check failed for $tp: ${err.message()}")
        false
    }
  }

  private sealed trait SweepClassification
  private case object SweepSkip extends SweepClassification
  private case class SweepNeedsRead(partitionKey: String) extends SweepClassification

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
      // A 0-byte listing entry (ADLS reports sizes) is read too: the delete needs the version it
      // was selected at, and the GET classifies what is stored now, so a lock repaired since the
      // listing is judged by its content (`readAndEnqueue`'s EmptyFileError arm keeps reaping
      // the 0-byte poison blobs of Bug B's non-atomic write).
      if (gcContainsKey(tp, partitionKey)) SweepSkip
      else SweepNeedsRead(partitionKey)
    }
  }

  /**
   * GETs the lock file and selects it for deletion (at the version read) if its committedOffset is strictly below
   * the master lock's committedOffset. The master lock stores `masterOffset = globalSafeOffset - 1`,
   * so `committedOffset < masterOffset` is equivalent to `committedOffset < globalSafeOffset - 1`,
   * matching the threshold used by `cleanUpObsoleteLocks`.
   *
   * The strict-less-than preserves the granular lock at `masterOffset` itself. On restart,
   * `context.offset(tp, masterOffset)` replays that record, and the granular lock is needed
   * by `shouldSkip` to deduplicate it (the one-record-overlap invariant).
   *
   * A lock with a PendingState is swept only when its `pendingOffset` is ALSO strictly below
   * `masterOffset`. A predecessor that crashed after the copy step has already produced a final
   * file up to `pendingOffset` while `committedOffset` stays old, and the master floor can pass
   * `committedOffset` without this key ever being loaded (and so recovered) by the new owner.
   * Replay starts at `masterOffset`, so a pending chain at or above it re-delivers records that
   * only this lock (resolved at load time) can deduplicate. Deleting it would write them twice.
   * A pending chain wholly below the floor is never replayed, so sweeping it is safe. The same
   * holds for a chain over no committed offset (a key's first commit).
   *
   * The epoch read rule applies here too: a lock written under a newer epoch than the one held
   * marks the partition fenced and is not queued (the drain also discards what a fenced partition
   * has queued), whatever its offsets say.
   */
  private def selectObsoleteLock(
    bucket:       String,
    path:         String,
    tp:           TopicPartition,
    partitionKey: String,
    masterOffset: Offset,
  ): Option[GcItem] =
    storageInterface.getBlobAsObject[IndexFile](bucket, path) match {
      case Left(EmptyFileError(_, eTag)) =>
        // 0-byte blob: classifyLockFile has already enforced the ageThreshold filter,
        // so this file is provably older than the worst-case create-flush window
        // and cannot be a live writer's in-progress .tmp. Deleted only while still this 0-byte
        // version: a writer that takes it over meanwhile keeps it.
        Some(GcItem(bucket, path, tp, partitionKey, eTag = Some(eTag)))
      case Left(err) =>
        logger.warn(s"Sweep: failed to read lock file $path: ${err.message()}")
        None
      case Right(ObjectWithETag(lock, _)) if rejectNewerEpoch(tp, partitionKey, lock).isLeft =>
        None
      case Right(ObjectWithETag(IndexFile(_, Some(committedOffset), pendingState, _), eTag))
          if committedOffset.value < masterOffset.value &&
            pendingState.forall(_.pendingOffset.value < masterOffset.value) =>
        Some(GcItem(bucket, path, tp, partitionKey, eTag = Some(eTag)))
      case Right(ObjectWithETag(IndexFile(_, None, Some(pending), _), eTag))
          if pending.pendingOffset.value < masterOffset.value =>
        // The shape a key's first commit writes (a chain recorded from no committed offset), left
        // by a task that died mid-chain. Same rule as a chain over a committed offset: wholly
        // below the replay start, it is never re-delivered.
        Some(GcItem(bucket, path, tp, partitionKey, eTag = Some(eTag)))
      case Right(ObjectWithETag(IndexFile(_, None, None, _), eTag)) =>
        // Empty lock created by ensureGranularLock but never committed to.
        // classifyLockFile already enforced the age threshold and cache absence,
        // so no active writer is using this file and it carries no durable data.
        // Without this branch, empty locks accumulate indefinitely under
        // high-cardinality PARTITIONBY with churning keys.
        Some(GcItem(bucket, path, tp, partitionKey, eTag = Some(eTag)))
      case _ =>
        None
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
   * that were enqueued but not yet drained, for partitions still held. On the normal
   * close(partitions) → stop() path `closePartitions` has already dropped their queued
   * items and released them, so this drain finds nothing of theirs. On a `stop()` without
   * `close(partitions)` the writers' close has evicted the granular cache, so the drain's reclaim
   * filter sees nothing; the drain is still safe because each lock delete is conditional on the
   * version selected: a key reclaimed and rewritten since (by this task or a new owner) keeps
   * its lock.
   *
   * The final `drainGcQueue()` call is direct (not through the scheduled lambda), so
   * it bypasses the `acceptingWork` gate and runs regardless of the flag's value.
   */
  override def close(): Unit = {
    // First, so a sweep that outlives the 5 s executor wait below deletes nothing (LC-349).
    ownedPartitions.clear()
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

  /** Result of one batch temp sweep delete chunk (LC-349). */
  private sealed trait ChunkOutcome
  private object ChunkOutcome {

    /** Stop the pass for this partition: not live, lock unreadable, or owned by another instance. */
    case object Stop extends ChunkOutcome
    final case class Done(deleted: Int, failed: Int) extends ChunkOutcome
  }

  case class GranularCacheEntry(offset: Option[Offset], eTag: String)

  /** A fence: the held ownership epoch it was placed against, and why. */
  private final case class Fence(epoch: Long, reason: String)

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
    // The version of the lock GC selected (`Lock` items): the delete is conditional on it.
    // `None` for `TmpOrphan` items, which are deleted by path.
    eTag: Option[String] = None,
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

  /** What a conditional delete that reported no outcome for an object is taken as: a failure. */
  private val MissingDeleteOutcome: DeleteOutcome =
    DeleteOutcome.Failed(FileDeleteError(new IllegalStateException("no outcome reported for the delete"), ""))

  /**
   * Bounded ownership writes `open()` makes on a master lock (creates and bumps alike, shared). Each
   * failed write is re-read, the last included, so the bound is on writes, not reads. One re-read
   * normally suffices — cloud storage is read-after-write consistent for the destination — so the
   * cap only guards against a pathological absent/present flicker or a run of failed writes before
   * surfacing a fatal error.
   */
  val MaxOpenCreateRaceAttempts: Int = 3

  /**
   * An ownership write `open()` made that failed. `writtenEpochs` holds the epoch it stamped and
   * the epochs of every earlier write by the same open, so a re-read can tell that any of them
   * landed. `spent` is the error the open fails with if that re-read is the last and decides nothing.
   */
  private[seek] final case class FailedOpenWrite(writtenEpochs: Set[Long], spent: SinkError)

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
   * The smallest per-pass read budget the granular orphan sweep can work with: one read for a lock
   * file and one for the master-lock re-check made before anything is enqueued. `IndexSettings`
   * raises a lower configured value to this.
   */
  val MinGcSweepMaxReads: Int = 2

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
   * Test-only access point for the test seams. Any production reference to
   * `IndexManagerV2TestHooks.*` is a bug; the name makes stray uses obvious under code review. See
   * the `testHookPreWriteMasterLockBarrier` and `testHookOwnershipPublished` field docs for semantics.
   */
  object IndexManagerV2TestHooks {
    def installPreWriteMasterLockBarrier(im: IndexManagerV2, barrier: () => Unit): Unit =
      im.testHookPreWriteMasterLockBarrier = barrier

    def clearPreWriteMasterLockBarrier(im: IndexManagerV2): Unit =
      im.testHookPreWriteMasterLockBarrier = () => ()

    def installOwnershipPublishedHook(im: IndexManagerV2, hook: (TopicPartition, Long) => Unit): Unit =
      im.testHookOwnershipPublished = hook

    def clearOwnershipPublishedHook(im: IndexManagerV2): Unit =
      im.testHookOwnershipPublished = (_, _) => ()

    /**
     * Drops the held ownership epoch alone, leaving the cached eTags in place: the state of the bug
     * (a write path that skipped `open()`, or state cleared under a live writer) the unstamped-write
     * refusal guards against. No production path clears the epoch without the eTag.
     */
    def dropHeldEpoch(im: IndexManagerV2, topicPartition: TopicPartition): Unit = {
      val _ = im.tpEpoch.remove(topicPartition)
    }
  }

}
