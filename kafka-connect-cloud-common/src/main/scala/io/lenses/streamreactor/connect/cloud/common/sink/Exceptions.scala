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
package io.lenses.streamreactor.connect.cloud.common.sink

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition

trait SinkError {
  def exception(): Option[Throwable]

  def message(): String

  def rollBack(): Boolean

  def topicPartitions(): Set[TopicPartition]
}

// Cannot be retried, must be cleaned up
case class FatalCloudSinkError(message: String, exception: Option[Throwable], topicPartition: TopicPartition)
    extends SinkError {

  override def rollBack(): Boolean = true

  override def topicPartitions(): Set[TopicPartition] = Set(topicPartition)
}

case object FatalCloudSinkError {

  def apply(message: String, topicPartition: TopicPartition): FatalCloudSinkError =
    FatalCloudSinkError(message, Option.empty, topicPartition)

}

/**
 * Represents a non-fatal error that occurred in the cloud sink.
 * Non-fatal errors can be retried and do not require a rollback.
 *
 * The retry path depends on context: Upload failures are retried by `recommitPending`
 * from the still-on-disk local file.
 *
 * When `swallowable = true` (the default), `NoopErrorPolicy` logs and continues — the
 * error is genuinely transient and safe to ignore (e.g. an upload failure where the
 * staging file is still on disk for `recommitPending` to retry).
 *
 * When `swallowable = false`, the error is integrity-sensitive (e.g. a transient cloud
 * read failure while loading a granular lock prevents the connector from knowing the
 * correct deduplication floor). In this case:
 *   - `error.policy=RETRY` wraps it in `RetriableException` so Kafka Connect re-delivers
 *     the same batch on the next `put()` call (see `RetryErrorPolicy`).
 *   - `error.policy=NOOP` and `error.policy=THROW` fail fast — the error is surfaced as a
 *     `RetriableIntegrityException` (a subclass of `FatalConnectException`) so NOOP does
 *     NOT silently swallow it and `preCommit` does NOT advance past the affected offsets.
 *
 * @param message       A descriptive message about the error.
 * @param exception     An optional exception associated with the error.
 * @param swallowable   When false, NOOP/THROW fail fast and RETRY schedules a re-delivery.
 */
case class NonFatalCloudSinkError(message: String, exception: Option[Throwable], swallowable: Boolean = true)
    extends SinkError {

  override def rollBack(): Boolean = false

  override def topicPartitions(): Set[TopicPartition] = Set()
}

case object NonFatalCloudSinkError {
  def apply(message: String): NonFatalCloudSinkError =
    NonFatalCloudSinkError(message, Option.empty)

  def apply(exception: Throwable): NonFatalCloudSinkError =
    NonFatalCloudSinkError(exception.getMessage, exception.some)

  /** Constructs a non-fatal error that must NOT be swallowed by NOOP or THROW. */
  def unswallowable(message: String, exception: Option[Throwable]): NonFatalCloudSinkError =
    NonFatalCloudSinkError(message, exception, swallowable = false)
}

case object BatchCloudSinkError {
  def apply(mixedExceptions: Set[SinkError]): BatchCloudSinkError =
    BatchCloudSinkError(
      mixedExceptions.collect {
        case fatal: FatalCloudSinkError => fatal
      },
      mixedExceptions.collect {
        case fatal: NonFatalCloudSinkError => fatal
      },
    )
}

case class BatchCloudSinkError(
  fatal:    Set[FatalCloudSinkError],
  nonFatal: Set[NonFatalCloudSinkError],
) extends SinkError {

  override def exception(): Option[Throwable] = {
    // Fully deterministic: (1) prefer the fatal set when non-empty -- the contract
    // CloudSinkTask.handleErrors and the FatalConnectException cause-chain assertion
    // depend on; (2) sort by (topic.value, partition) so multi-fatal batches surface
    // the SAME fatal cause across runs (consistent log filters, deterministic
    // observability metrics that key on the cause's identity / TP / message / top of
    // stack trace). Cost: O(n log n) on a typically tiny set (<= number of TPs in a
    // single Connect batch); negligible vs. the eTag-CAS and cloud I/O on the same
    // code path. Without (2), Set.headOption hash iteration order means downstream
    // alert rules and dashboards keying on the cause are flaky in multi-fatal batches.
    val fatalCause: Option[Throwable] =
      fatal.toList
        .sortBy(e => (e.topicPartition.topic.value, e.topicPartition.partition))
        .headOption
        .flatMap(e => (e: SinkError).exception())
    // Sort by (message, cause.toString) for full determinism when two entries share the same
    // message but have different causes.  The secondary key uses toString (class + message) so
    // that lexicographic ordering is stable across JVM runs, unlike Object.hashCode.
    lazy val nonFatalCause: Option[Throwable] =
      nonFatal.toList
        .sortBy(e => (e.message, e.exception.map(_.toString).getOrElse("")))
        .headOption
        .flatMap(e => (e: SinkError).exception())
    if (fatal.nonEmpty) fatalCause else nonFatalCause
  }

  override def message(): String =
    "fatal:\n" + fatal.map(_.message).mkString("\n") + "\n\nnonFatal:\n" + nonFatal.map(_.message).mkString(
      "\n",
    ) + "\n\nFatal TPs:\n" + fatal.map(_.topicPartitions())

  override def rollBack(): Boolean = fatal.nonEmpty

  override def topicPartitions(): Set[TopicPartition] = fatal.map(_.topicPartition)

  /** True when the batch contains at least one non-fatal error that must not be swallowed. */
  def hasUnswallowable: Boolean = nonFatal.exists(!_.swallowable)
}
