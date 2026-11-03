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
package io.lenses.streamreactor.connect.benchmarks.http

import cats.data.NonEmptySeq
import cats.effect.IO
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.std.Queue
import cats.effect.std.Semaphore
import cats.implicits.toFoldableOps
import io.lenses.streamreactor.common.batch.BatchPolicy
import io.lenses.streamreactor.common.batch.Count
import io.lenses.streamreactor.common.batch.HttpCommitContext
import io.lenses.streamreactor.connect.benchmarks.BenchResult
import io.lenses.streamreactor.connect.benchmarks.HeapSampler
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.http.sink.HttpWriter
import io.lenses.streamreactor.connect.http.sink.client.NoAuthenticationHttpRequestSender
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfig
import io.lenses.streamreactor.connect.http.sink.metrics.HttpSinkMetrics
import io.lenses.streamreactor.connect.http.sink.tpl.RawTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import io.lenses.streamreactor.connect.http.sink.tpl.TemplateType
import org.apache.kafka.connect.sink.SinkRecord
import org.http4s.Response
import org.http4s.Status
import org.http4s.client.Client

import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

/**
 * Drives the HTTP sink's production render -> queue -> batch -> send pipeline for a single
 * (single-partition) topic, with Kafka and the network removed:
 *
 *  - "Kafka removed": records are handed directly to the pipeline as `SinkRecord`s, exactly as
 *    `HttpSinkTask.put()` would receive them from Kafka Connect; no consumer/broker is involved.
 *  - "Network removed": the `org.http4s.client.Client[IO]` at the bottom of the stack is a stub
 *    that returns `200 OK` (optionally after a configurable delay), so no bytes ever leave the
 *    process. Everything above that seam -- template rendering, the per-topic `RecordsQueue`,
 *    batching policy evaluation, request/body assembly, and the real `HttpSinkMetrics` -- is
 *    unmodified production code.
 *
 * Design note on the drain loop: production drives the pipeline from a single long-lived consumer
 * fiber per topic (`HttpWriter.consume()`), started by `HttpWriterManager`. This harness starts the
 * same consumer fiber and feeds records into the writer's bounded queue via `HttpWriter.add`, then
 * waits until the queue has drained and every batch has been sent (tracked via the success-count
 * metric). This measures the pipeline's unconstrained throughput ceiling.
 */
class HttpSinkThroughputHarness {

  private val sinkName = "bench-http"

  /**
   * @param props           HTTP sink connector properties (same keys/semantics as production,
   *                         e.g. `connect.http.batch.count`), minus `connect.http.endpoint`/
   *                         `connect.http.request.content`/`connect.http.method` which are
   *                         fixed by this harness.
   * @param records         input records; `records.length` MUST be an exact multiple of the
   *                         effective batch count so the final partial batch is not left
   *                         stranded in the queue (see [[HttpSinkThroughputHarness]] scaladoc).
   * @param egressLatency   simulated network/server latency added to every stubbed HTTP response.
   * @param requestContent  the `connect.http.request.content` template; defaults to a simple
   *                         pass-through template.
   */
  def run(
    scenario:       String,
    props:          Map[String, String],
    records:        IndexedSeq[SinkRecord],
    egressLatency:  FiniteDuration = Duration.Zero,
    requestContent: String         = "{{#message}}{{value}}\n{{/message}}",
    pollChunkSize:  Int            = 500,
  ): IO[BenchResult] = {
    val fullProps = props ++ Map(
      "connect.http.endpoint"        -> "http://bench.invalid/mocked",
      "connect.http.request.content" -> requestContent,
      "connect.http.method"          -> "POST",
    )

    for {
      config  <- IO(HttpSinkConfig.from(fullProps)).rethrow
      metrics <- IO(new HttpSinkMetrics())
      template = RawTemplate(config.endpoint, config.content, config.headers, config.nullPayloadHandler)
      sender =
        new NoAuthenticationHttpRequestSender(sinkName,
                                              config.method.toHttp4sMethod,
                                              stubClient(egressLatency),
                                              metrics,
        )
      batchPolicy = config.batch.toBatchPolicy
      writerAndQueue <- buildWriter(
        sender,
        template,
        batchPolicy,
        config.maxQueueSize,
        config.maxQueueOfferTimeout,
        metrics,
      )
      (writer, queue)  = writerAndQueue
      batchCount       = batchPolicy.conditions.collectFirst { case Count(c) => c.toInt }.getOrElse(records.length)
      expectedRequests = math.ceil(records.length.toDouble / math.max(1, batchCount)).toInt
      heapBefore      <- IO(HeapSampler.usedHeapBytes())
      start           <- IO.monotonic
      // The consumer fiber runs for the duration of the `use` block and is cancelled on exit.
      // `elapsed` is captured inside the block so the cancellation is excluded from the timed window.
      elapsed <- writer.consume().background.use { _ =>
        feedAll(writer, template, records, pollChunkSize) *>
          awaitDrained(queue, metrics, expectedRequests) *>
          IO.monotonic.map(_ - start)
      }
      heapAfter <- IO(HeapSampler.usedHeapBytes())
    } yield BenchResult(
      sink               = "HTTP",
      scenario           = scenario,
      records            = records.length.toLong,
      elapsedNanos       = elapsed.toNanos,
      networkOps         = metrics.get2xxCount,
      flushes            = metrics.get2xxCount, // one HTTP request per batch: request == flush
      heapUsedDeltaBytes = math.max(0L, heapAfter - heapBefore),
    )
  }

  private def feedAll(
    writer:        HttpWriter,
    template:      TemplateType,
    records:       IndexedSeq[SinkRecord],
    pollChunkSize: Int,
  ): IO[Unit] =
    records.grouped(math.max(1, pollChunkSize)).toList.traverse_(chunk => putChunk(writer, template, chunk))

  private def stubClient(latency: FiniteDuration): Client[IO] =
    Client[IO] { _ =>
      val ok = IO.pure(Response[IO](Status.Ok).withEntity("OK"))
      Resource.eval(if (latency <= Duration.Zero) ok else IO.sleep(latency) *> ok)
    }

  private def buildWriter(
    sender:               NoAuthenticationHttpRequestSender,
    template:             TemplateType,
    batchPolicy:          BatchPolicy,
    maxQueueSize:         Int,
    maxQueueOfferTimeout: FiniteDuration,
    metrics:              HttpSinkMetrics,
  ): IO[(HttpWriter, Queue[IO, NonEmptySeq[RenderedRecord]])] =
    for {
      recordsQueue     <- Queue.unbounded[IO, NonEmptySeq[RenderedRecord]]
      permits          <- Semaphore[IO](maxQueueSize.toLong)
      commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default(sinkName))
      offsetsRef       <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
      writer = new HttpWriter(
        sinkName             = sinkName,
        sender               = sender,
        template             = template,
        batchPolicy          = batchPolicy,
        recordsQueue         = recordsQueue,
        permits              = permits,
        maxQueueSize         = maxQueueSize,
        offsetMapRef         = offsetsRef,
        maxQueueOfferTimeout = maxQueueOfferTimeout,
        errorThreshold       = Int.MaxValue,
        tidyJson             = false,
        errorReporter        = NoopReporters.error,
        successReporter      = NoopReporters.success,
        commitContextRef     = commitContextRef,
        metrics              = metrics,
      )
    } yield (writer, recordsQueue)

  private def putChunk(writer: HttpWriter, template: TemplateType, chunk: IndexedSeq[SinkRecord]): IO[Unit] =
    NonEmptySeq.fromSeq(chunk.toList) match {
      case None => IO.unit
      case Some(nes) =>
        template.renderRecords(nes) match {
          case Left(err)       => IO.raiseError(err)
          case Right(rendered) => writer.add(rendered)
        }
    }

  // The consumer fiber runs concurrently; the run is complete once the queue is empty and every
  // expected batch has been sent (the stub client reports each send as a 2xx).
  private def awaitDrained(
    queue:            Queue[IO, NonEmptySeq[RenderedRecord]],
    metrics:          HttpSinkMetrics,
    expectedRequests: Int,
  ): IO[Unit] = {
    def loop(): IO[Unit] =
      for {
        size <- queue.size
        sent <- IO(metrics.get2xxCount)
        _    <- if (size == 0 && sent >= expectedRequests) IO.unit else IO.sleep(1.milli) *> loop()
      } yield ()
    loop()
  }
}
