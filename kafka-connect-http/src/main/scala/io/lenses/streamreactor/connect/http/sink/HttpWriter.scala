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
package io.lenses.streamreactor.connect.http.sink

import cats.data.NonEmptySeq
import cats.effect.IO
import cats.effect.Ref
import cats.effect.std.Queue
import cats.effect.std.Semaphore
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import cats.implicits.toFoldableOps
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.batch.BatchPolicy
import io.lenses.streamreactor.common.batch.HttpCommitContext
import io.lenses.streamreactor.common.batch.OffsetMergeUtils
import io.lenses.streamreactor.common.utils.CyclopsToScalaOption.convertToCyclopsOption
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.http.sink.client.HttpRequestSender
import io.lenses.streamreactor.connect.http.sink.client.HttpResponseFailure
import io.lenses.streamreactor.connect.http.sink.client.HttpResponseSuccess
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpFailureConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpSuccessConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import io.lenses.streamreactor.connect.http.sink.tpl.TemplateType
import io.lenses.streamreactor.connect.reporting.ReportingController
import io.lenses.streamreactor.connect.reporting.model.ConnectorSpecificRecordData
import io.lenses.streamreactor.connect.reporting.model.ReportingRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.errors.RetriableException

import scala.concurrent.duration.DurationLong
import scala.concurrent.duration.FiniteDuration

object HttpWriter {

  /**
   * Allocates the writer's internal state and constructs a production writer.
   *
   * The semaphore has exactly `maxQueueSize` permits, matching the chunk cap in `enqueueGroups`.
   * Keeping those values paired guarantees that a chunk never requests more permits than can
   * exist. A permit is held for the entire lifetime of a record inside the writer -- from enqueue
   * until it is flushed (sent) or dropped -- so the permit count bounds queue + accumulator +
   * in-flight records together. Because permits are held through the accumulator, the batch record
   * count trigger must not exceed `maxQueueSize` (validated at config time), otherwise a batch
   * could never fill while producers are blocked. Direct construction remains available for tests
   * and benchmarks that inject or observe the internal queue and references.
   */
  def create(
    sinkName:             String,
    sender:               HttpRequestSender,
    template:             TemplateType,
    batchPolicy:          BatchPolicy,
    maxQueueSize:         Int,
    maxQueueOfferTimeout: FiniteDuration,
    errorThreshold:       Int,
    tidyJson:             Boolean,
    errorReporter:        ReportingController[HttpFailureConnectorSpecificRecordData],
    successReporter:      ReportingController[HttpSuccessConnectorSpecificRecordData],
  ): IO[HttpWriter] =
    for {
      recordsQueue     <- Queue.unbounded[IO, NonEmptySeq[RenderedRecord]]
      permits          <- Semaphore[IO](maxQueueSize.toLong)
      commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default(sinkName))
      offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
    } yield new HttpWriter(
      sinkName             = sinkName,
      sender               = sender,
      template             = template,
      batchPolicy          = batchPolicy,
      recordsQueue         = recordsQueue,
      permits              = permits,
      maxQueueSize         = maxQueueSize,
      offsetMapRef         = offsetMapRef,
      maxQueueOfferTimeout = maxQueueOfferTimeout,
      errorThreshold       = errorThreshold,
      tidyJson             = tidyJson,
      errorReporter        = errorReporter,
      successReporter      = successReporter,
      commitContextRef     = commitContextRef,
    )
}

/**
 * The `HttpWriter` owns a single topic's ingest-to-send pipeline.
 *
 * Records enter via [[add]] (called on the Kafka Connect `put` thread), which hands whole chunks of
 * records to a concurrent [[cats.effect.std.Queue]]. Backpressure is enforced by a
 * [[cats.effect.std.Semaphore]] whose permits count individual records (capped at `maxQueueSize`):
 * a producer acquires one permit per record before enqueuing, blocking until capacity frees up or
 * `maxQueueOfferTimeout` elapses, at which point a `RetriableException` is raised (so Kafka
 * Connect's retry configuration takes over). Passing records as chunks keeps the number of queue
 * operations and fiber hand-offs proportional to the number of `put` calls, not the number of
 * records.
 *
 * Permit acquisition and the queue offer are atomic with respect to cancellation: once permits are
 * acquired they are always paired with an enqueued chunk (and, symmetrically, a chunk's permits are
 * always released once the batch has been sent), so a cancellation can never strand permits with no
 * chunk for the consumer to release them against.
 *
 * A single long-lived consumer fiber (started by `HttpWriterManager`) drives [[consume]], which
 * takes a chunk, feeds the records into a mutable [[BatchAccumulator]] and flushes a batch whenever
 * the configured [[BatchPolicy]] triggers (by count, size, or a time-based interval deadline). A
 * record's permit is held for its whole lifetime in the writer -- it is released only when the
 * record leaves the pipeline (flushed on a successful send, or dropped on a below-threshold
 * failure), so the semaphore bounds queue + accumulator + in-flight records together rather than
 * just the queue. Because a permit is held while a record sits in the accumulator, the batch record
 * count trigger must not exceed `maxQueueSize` (enforced by config validation) or a batch could
 * never fill while producers are blocked on permits.
 *
 * Production code should prefer [[HttpWriter.create]]. Direct construction is retained for tests
 * and benchmarks that need to inject or observe the writer's internal state.
 */
class HttpWriter(
  sinkName:             String,
  sender:               HttpRequestSender,
  template:             TemplateType,
  batchPolicy:          BatchPolicy,
  recordsQueue:         Queue[IO, NonEmptySeq[RenderedRecord]],
  permits:              Semaphore[IO],
  maxQueueSize:         Int,
  offsetMapRef:         Ref[IO, Map[TopicPartition, Offset]],
  maxQueueOfferTimeout: FiniteDuration,
  errorThreshold:       Int,
  tidyJson:             Boolean,
  errorReporter:        ReportingController[HttpFailureConnectorSpecificRecordData],
  successReporter:      ReportingController[HttpSuccessConnectorSpecificRecordData],
  commitContextRef:     Ref[IO, HttpCommitContext],
) extends LazyLogging {

  // TODO: feedback to kafka a warning if the queue gets too large

  /**
   * Enqueues records for asynchronous processing. Records whose offset was already accepted are
   * discarded (deduplication). The remainder are enqueued as chunks under the record-counting
   * backpressure semaphore; if capacity does not free up within `maxQueueOfferTimeout` a
   * `RetriableException` is raised.
   */
  def add(newRecords: NonEmptySeq[RenderedRecord]): IO[Unit] =
    offsetMapRef.get.flatMap { offsetMap =>
      enqueueGroups(filterDuplicates(newRecords.toSeq.toList, offsetMap))
    }

  /**
   * Clears the dedup high-water mark for the given partitions. Called when partitions are
   * (re)assigned (Kafka rewinds the consumer to the last committed offset), so redelivered records
   * are accepted again by [[filterDuplicates]] instead of being silently discarded as duplicates.
   */
  def resetAcceptedOffsets(partitions: Set[TopicPartition]): IO[Unit] =
    if (partitions.isEmpty) IO.unit
    else offsetMapRef.update(_ -- partitions)

  private def filterDuplicates(
    records:   List[RenderedRecord],
    offsetMap: Map[TopicPartition, Offset],
  ): List[RenderedRecord] =
    records.filter { record =>
      val tp = record.topicPartitionOffset.toTopicPartition
      offsetMap.get(tp) match {
        case Some(lastOffset) if record.topicPartitionOffset.offset.value <= lastOffset.value => false
        case _                                                                                => true
      }
    }

  // Enqueue the records as chunks. Each chunk is capped at `maxQueueSize` records so a single
  // oversized `put` cannot request more permits than exist (which would deadlock).
  private def enqueueGroups(records: List[RenderedRecord]): IO[Unit] =
    if (records.isEmpty) IO.unit
    else
      records.grouped(maxQueueSize).toList.traverse_ { group =>
        val chunk = NonEmptySeq.fromSeqUnsafe(group)
        tryEnqueueNow(chunk, group).flatMap {
          case true  => IO.unit
          case false => awaitCapacityAndEnqueue(chunk, group)
        }
      }

  // Fast path: reserve all permits without blocking. `tryAcquireN` and the offer share one
  // uncancelable region, so cancellation can never land between them (which would strand permits
  // with no queued chunk for the consumer to release them against).
  private def tryEnqueueNow(chunk: NonEmptySeq[RenderedRecord], group: List[RenderedRecord]): IO[Boolean] =
    IO.uncancelable { _ =>
      permits.tryAcquireN(group.size.toLong).flatMap {
        case true  => enqueue(chunk, group).as(true)
        case false => IO.pure(false)
      }
    }

  // Slow path: only the wait for capacity is cancelable. `timeoutTo` runs this block in a fresh
  // `racePair` child fiber, so the `IO.uncancelable`/`poll` pair belongs to that child: the wait is
  // cancelable, but everything after the acquire returns is masked. `Semaphore.acquireN` hands back
  // any permits it was granted if it is cancelled while waiting; once it returns normally, the mask
  // carries through to the offer, so neither the timeout firing nor an outer cancellation can
  // separate granted permits from their chunk. One timeout budget per put call (not per record).
  private def awaitCapacityAndEnqueue(chunk: NonEmptySeq[RenderedRecord], group: List[RenderedRecord]): IO[Unit] =
    IO.uncancelable { poll =>
      poll(permits.acquireN(group.size.toLong)) *> enqueue(chunk, group)
    }.timeoutTo(
      maxQueueOfferTimeout,
      IO.raiseError(new RetriableException("Enqueue timed out and records remain")),
    )

  // The offer relies on `recordsQueue` being unbounded (`HttpWriter.create` uses `Queue.unbounded`;
  // the semaphore, not the queue, is what bounds occupancy). A bounded queue injected via direct
  // construction would make this masked `offer` block uninterruptibly once full.
  private def enqueue(chunk: NonEmptySeq[RenderedRecord], group: List[RenderedRecord]): IO[Unit] =
    recordsQueue.offer(chunk) *> recordThePartitionOffsets(group)

  // Advances the per-partition max offset for a whole set of accepted records in a single atomic
  // update, rather than one `Ref` update per record.
  private def recordThePartitionOffsets(records: List[RenderedRecord]): IO[Unit] =
    if (records.isEmpty) IO.unit
    else
      offsetMapRef.update { offsetMap =>
        records.foldLeft(offsetMap) { (acc, record) =>
          val tp     = record.topicPartitionOffset.toTopicPartition
          val offset = record.topicPartitionOffset.offset
          val updatedOffset: Offset = acc.get(tp) match {
            case Some(existingOffset) if existingOffset.value >= offset.value => existingOffset
            case _                                                            => offset
          }
          acc.updated(tp, updatedOffset)
        }
      }

  /**
   * The consumer loop. Runs until cancelled. Takes chunks off the queue, folds their records into a
   * mutable [[BatchAccumulator]] and flushes according to the batch policy.
   */
  def consume(): IO[Unit] =
    commitContextRef.get.flatMap(ctx => loop(new BatchAccumulator(batchPolicy, ctx)))

  private def loop(acc: BatchAccumulator): IO[Unit] =
    nextChunk(acc).flatMap {
      case Some(chunk) =>
        // Permits are NOT released here: they follow the records into the accumulator and are only
        // released once the records actually leave the pipeline (flushed on a successful send or
        // dropped on a below-threshold failure). This keeps queue + accumulator + in-flight bounded
        // by `maxQueueSize`.
        processChunk(acc, chunk) *> loop(acc)
      case None =>
        flushIfNonEmpty(acc) *> loop(acc)
    }

  /**
   * Waits for the next chunk. When the batch is empty (or the policy has no interval condition) it
   * blocks indefinitely on the queue. Otherwise it races the next chunk against the interval-based
   * flush deadline, returning `None` when the deadline fires first.
   */
  private def nextChunk(acc: BatchAccumulator): IO[Option[NonEmptySeq[RenderedRecord]]] =
    // `IO.defer` so the branch decision reads the accumulator at execution time, not when this IO is
    // constructed. `loop` builds the recursive `... *> loop(acc)` step (which calls `nextChunk`)
    // before `processChunk` has folded the chunk into the accumulator, so without deferring, an
    // empty-accumulator snapshot would be captured and the interval deadline branch would never be
    // taken for records that were just accumulated -- time-based flushes would never fire.
    IO.defer {
      if (acc.isEmpty) recordsQueue.take.map(_.some)
      else acc.nextFlushDeadlineMillis match {
        case None => recordsQueue.take.map(_.some)
        case Some(deadline) =>
          IO.realTime.flatMap { now =>
            val waitMillis = math.max(0L, deadline - now.toMillis)
            IO.race(recordsQueue.take, IO.sleep(waitMillis.millis)).map {
              case Left(chunk) => chunk.some
              case Right(_)    => none
            }
          }
      }
    }

  /**
   * Folds a chunk into the accumulator, then flushes only once nothing more is immediately
   * available. A `greedyTriggerReached` (interval-elapsed) result does not force a flush on its own:
   * doing so would cut the chunk short into a one-record batch and re-arm the interval for the rest.
   * Instead the chunk is packed until a hard `triggerReached` (count/size) fires or the records run
   * out, and if the interval is still pending we drain any already-queued chunks before flushing.
   * This mirrors the pack-until-full behaviour of
   * `io.lenses.streamreactor.common.batch.RecordsQueueBatcher.takeBatch`.
   */
  private def processChunk(acc: BatchAccumulator, chunk: NonEmptySeq[RenderedRecord]): IO[Unit] =
    offerAll(acc, chunk.toSeq.iterator).flatMap(greedy => if (greedy) drainAndFlush(acc) else IO.unit)

  /**
   * Offers every record from `iterator` into the accumulator, returning whether a greedy (interval)
   * trigger is pending and still unflushed. Records are offered synchronously inside `IO.defer` and
   * an `IO` is materialised only when a flush is actually required, so the common "fits, no flush"
   * record incurs no per-record `IO` allocation. The iterator is stateful and shared across
   * `step()` recursions, so each recursion resumes where the previous flush segment stopped. A flush
   * satisfies any pending greedy state, so the greedy flag is cleared once a flush has run.
   */
  private def offerAll(acc: BatchAccumulator, iterator: Iterator[RenderedRecord]): IO[Boolean] = {
    def step(): IO[Boolean] = IO.defer {
      var greedy = false
      // `next` is a null sentinel: null means "no flush IO needed yet in this segment". `eq null` is a
      // pure reference-identity check (no `.equals` dispatch), keeping the common "fits, no flush" record
      // on the allocation-free path described above.
      var next: IO[Boolean] = null
      while (iterator.hasNext && (next eq null)) {
        val record = iterator.next()
        val result = acc.offer(record)
        if (result.fitsInBatch) {
          if (result.triggerReached) next = flushIfNonEmpty(acc).as(false)
          else if (result.greedyTriggerReached) greedy = true
        } else {
          // record does not fit the current batch: flush what we have, then place it in a fresh batch
          next = flushIfNonEmpty(acc) *> reofferAfterFlush(acc, record)
        }
      }
      if (next eq null) IO.pure(greedy) else next.flatMap(g => step().map(_ || g))
    }

    step()
  }

  // A greedy (interval) trigger is pending: rather than flush a short batch, pull any chunks already
  // sitting in the queue (non-blocking) and pack them in too, flushing only once the queue is empty.
  private def drainAndFlush(acc: BatchAccumulator): IO[Unit] =
    recordsQueue.tryTake.flatMap {
      case Some(next) => offerAll(acc, next.toSeq.iterator).flatMap(g => if (g) drainAndFlush(acc) else IO.unit)
      case None       => flushIfNonEmpty(acc)
    }

  private def reofferAfterFlush(acc: BatchAccumulator, record: RenderedRecord): IO[Boolean] =
    IO(acc.offer(record)).flatMap { result =>
      if (result.fitsInBatch) {
        if (result.triggerReached) flushIfNonEmpty(acc).as(false) else IO.pure(result.greedyTriggerReached)
      } else {
        // even an empty batch rejects it (e.g. a single record over the size limit): send it alone
        flushSingle(acc, record).as(false)
      }
    }

  private def flushIfNonEmpty(acc: BatchAccumulator): IO[Unit] =
    if (acc.isEmpty) IO.unit else flush(acc)

  // The send result and the bookkeeping that acts on it (commit context, accumulator reset, permit
  // release, or the error handling) must stay in lockstep: only `sendBatch` is polled (so an
  // in-flight request is still cancelled promptly at shutdown), while the result handling is masked
  // so a cancellation cannot land on the `attempt` bind and skip the permit release, stranding
  // permits for records that have already left the pipeline.
  //
  // Cancellation reaching the polled `sendBatch` is deliberate, and skipping the release there is
  // correct rather than a leak. In production the only cancellation source is the
  // `IO.race(consume(), deferred.get)` in `HttpWriterManager.startConsumer`, and `deferred` is
  // completed only by `HttpSinkTask.stop()`, so it happens only at shutdown. `.attempt` does not
  // catch cancellation (in CE3 it is a separate channel from errors), so the masked continuation --
  // `releaseN` included -- never runs. The batch stays in the accumulator and the commit context is
  // untouched, so those records have not left the pipeline and correctly keep holding their
  // permits, and because offsets never advance Kafka redelivers them. Note this means the send was
  // not *confirmed*, not that it did not happen: the request may already have reached the endpoint,
  // so redelivery can duplicate it -- the usual at-least-once trade-off.
  //
  // Permits held this way cannot wedge a producer: `awaitCapacityAndEnqueue` bounds its wait with
  // `maxQueueOfferTimeout` and fails with `RetriableException`, so exhausted capacity degrades to a
  // Connect-level retry rather than a deadlock -- and at shutdown the semaphore is discarded with
  // the writer anyway.
  //
  // A scoped `permit.use` / acquire-with-guaranteed-release does not fit: permits are acquired in
  // `add` (producer, the Connect `put` thread) and released here on the consumer fiber, a hand-off
  // across the queue and accumulator rather than a scoped resource.
  private def flush(acc: BatchAccumulator): IO[Unit] =
    acc.currentBatch match {
      case None => IO.unit
      case Some(batch) =>
        val flushedCount = batch.length.toLong
        // Log the once-per-flush explanation here (not in the batch policy) so the line describes the
        // batch actually being sent, covering hard, greedy and interval-deadline flushes uniformly.
        IO(acc.logFlush()) *> IO.uncancelable { poll =>
          poll(sendBatch(batch).attempt).flatMap {
            case Right(_) =>
              val flushedCtx = acc.flushedContext().resetErrors
              // These records have left the pipeline (sent), so return their permits to the semaphore.
              commitContextRef.set(flushedCtx) *> IO(acc.resetTo(flushedCtx)) *> permits.releaseN(flushedCount)
            case Left(error) => handleFlushError(acc, error, flushedCount)
          }
        }
    }

  // Cancellation and permit accounting behave exactly as described on `flush` above.
  private def flushSingle(acc: BatchAccumulator, record: RenderedRecord): IO[Unit] =
    IO(acc.logFlushSingle(record)) *> IO.uncancelable { poll =>
      poll(sendBatch(NonEmptySeq.of(record)).attempt).flatMap {
        case Right(_) =>
          val flushedCtx = acc.candidateContextFor(record).resetErrors
          // The single record has left the pipeline (sent), so return its permit to the semaphore.
          commitContextRef.set(flushedCtx) *> IO(acc.resetTo(flushedCtx)) *> permits.release
        case Left(error) => handleFlushError(acc, error, 1L)
      }
    }

  /**
   * On a flush failure the error is recorded against the commit context. The sink tolerates exactly
   * `errorThreshold` consecutive failed flushes (per topic-partition, reset on the next successful
   * flush); the failure after that -- the `errorThreshold + 1`th -- is re-raised, propagating out of
   * the consumer fiber to the task's error callback. Below that boundary the error is logged and the
   * failed batch is dropped without advancing committed offsets, so Kafka will redeliver those
   * records.
   *
   * `flushedCount` permits are returned to the semaphore in both cases: the batch leaves the
   * accumulator whether it is dropped (below threshold) or the fiber is torn down (at threshold).
   */
  private def handleFlushError(acc: BatchAccumulator, error: Throwable, flushedCount: Long): IO[Unit] =
    addErrorToCommitContext(error).flatMap {
      case Some(_) =>
        IO(logger.error(s"[$sinkName] Error in HttpWriter", error)) *> permits.releaseN(flushedCount) *>
          IO.raiseError(error)
      case None =>
        IO(logger.error(s"[$sinkName] Error in HttpWriter but not reached threshold so ignoring", error)) *>
          commitContextRef.get.flatMap(ctx => IO(acc.resetTo(ctx))) *> permits.releaseN(flushedCount)
    }

  private def sendBatch(records: NonEmptySeq[RenderedRecord]): IO[ProcessedTemplate] =
    for {
      _          <- IO.delay(logger.debug(s"[$sinkName] flushing batch of ${records.length}"))
      processed  <- IO.fromEither(template.process(records, tidyJson))
      httpResult <- sender.sendHttpRequest(processed)
      _          <- reportResult(records, processed, httpResult)
    } yield processed

  /**
   * Reports, per topic-partition, the offset Kafka should resume from.
   *
   * `HttpCommitContext.committedOffsets` holds the offset of the last record actually sent (see
   * `OffsetMergeUtils.maxOffsets`), but a Kafka committed offset means "the next offset to consume",
   * which is why `WorkerSinkTask` builds its own view as `record.kafkaOffset() + 1`. Connect commits
   * whatever this method returns verbatim, so the stored offset must be advanced by one here.
   *
   * Returning the raw offset instead would leave the group's committed offset one short of the log
   * end: a permanent lag of 1 per partition that never drains, and -- because `open()` clears the
   * dedup marks that `filterDuplicates` relies on -- the last record of each partition would be
   * redelivered and re-sent on every restart or rebalance.
   */
  def preCommit(
    initialOffsetAndMetaMap: Map[TopicPartition, OffsetAndMetadata],
  ): IO[Map[TopicPartition, OffsetAndMetadata]] =
    commitContextRef.get.map {
      case HttpCommitContext(_, committedOffsets, _, _, _, _, _) =>
        committedOffsets.flatMap {
          case (tp, offset) =>
            for {
              initialOffsetAndMeta <- initialOffsetAndMetaMap.get(tp)

            } yield tp -> new OffsetAndMetadata(offset.value + 1L,
                                                initialOffsetAndMeta.leaderEpoch(),
                                                initialOffsetAndMeta.metadata(),
            )
        }
      case _ => initialOffsetAndMetaMap
    }.orElse(IO(Map.empty[TopicPartition, OffsetAndMetadata]))

  // Use `updateAndGet` (not `getAndUpdate`): the threshold must be tested against the context that
  // already includes the current failure, otherwise it is evaluated against a count one short and
  // the sink tolerates one more failure than `errorThreshold` names.
  private def addErrorToCommitContext(e: Throwable): IO[Option[Throwable]] =
    commitContextRef.updateAndGet {
      commitContext => commitContext.addError(e)
    }.map(cc =>
      cc
        .errors
        .maxByOption { case (_, errSeq) => errSeq.size }
        .filter { case (_, errSeq) => errSeq.size > errorThreshold }
        .flatMap {
          case (_, errSeq) => errSeq.headOption
        },
    )

  private def reportResult(
    renderedRecords:   NonEmptySeq[RenderedRecord],
    processedTemplate: ProcessedTemplate,
    responseIo:        Either[HttpResponseFailure, HttpResponseSuccess],
  ): IO[Unit] = {
    val maxRecord = OffsetMergeUtils.maxRecord(renderedRecords.toSeq)

    def reportRecord[C <: ConnectorSpecificRecordData]: C => ReportingRecord[C] = (connectorSpecific: C) =>
      new ReportingRecord[C](
        maxRecord.topicPartitionOffset.toTopicPartition.toKafka,
        maxRecord.topicPartitionOffset.offset.value,
        maxRecord.timestamp,
        processedTemplate.endpoint,
        processedTemplate.content,
        connectorSpecific,
      )

    responseIo match {
      case Left(error) => IO(
          errorReporter.enqueue(
            reportRecord[HttpFailureConnectorSpecificRecordData](
              HttpFailureConnectorSpecificRecordData(
                convertToCyclopsOption(error.statusCode).map(_.toInt),
                convertToCyclopsOption(error.responseContent),
                error.getMessage,
              ),
            ),
          ),
        ) *> IO.raiseError(error)
      case Right(success) => IO(
          successReporter.enqueue(
            reportRecord[HttpSuccessConnectorSpecificRecordData](
              HttpSuccessConnectorSpecificRecordData(
                success.statusCode,
                convertToCyclopsOption(success.responseContent),
              ),
            ),
          ),
        )
    }
  }

}
