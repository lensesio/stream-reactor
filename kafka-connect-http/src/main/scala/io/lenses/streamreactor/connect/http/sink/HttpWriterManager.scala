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

import cats.effect.FiberIO
import cats.effect.IO
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.kernel.Deferred
import cats.effect.kernel.Temporal
import cats.effect.std.Semaphore
import cats.implicits.toFoldableOps
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.util.EitherUtils.unpackOrThrow
import io.lenses.streamreactor.common.utils.CyclopsToScalaOption.convertToScalaOption
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.http.sink.client.HttpRequestSender
import io.lenses.streamreactor.common.batch.BatchPolicy
import io.lenses.streamreactor.connect.http.sink.config.ExponentialRetryConfig
import io.lenses.streamreactor.connect.http.sink.config.FixedRetryConfig
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfig
import io.lenses.streamreactor.connect.http.sink.metrics.HttpSinkMetricsMBean
import io.lenses.streamreactor.connect.http.sink.metrics.MetricsResetter
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpFailureConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpSuccessConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.tpl.TemplateType
import io.lenses.streamreactor.connect.reporting.ReportingController
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.http4s.Response
import org.http4s.WaitQueueTimeoutException
import org.http4s.client.Client
import org.http4s.client.middleware.Retry
import org.http4s.client.middleware.RetryPolicy
import org.http4s.jdkhttpclient.JdkHttpClient

import java.net.http.HttpClient
import java.time.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

/**
 * The `HttpWriterManager` object provides a factory method to create an instance of `HttpWriterManager`.
 */
object HttpWriterManager extends StrictLogging {

  /**
   * Creates an instance of `HttpWriterManager`.
   *
   * @param sinkName The name of the sink.
   * @param config The HTTP sink configuration.
   * @param template The template type.
   * @param terminate A deferred value to signal termination.
   * @param t An implicit `Temporal` instance.
   * @return An `IO` action that creates an `HttpWriterManager`.
   */
  def apply(
    sinkName:  String,
    config:    HttpSinkConfig,
    template:  TemplateType,
    terminate: Deferred[IO, Either[Throwable, Unit]],
    metrics:   HttpSinkMetricsMBean,
  )(
    implicit
    t: Temporal[IO],
  ): IO[HttpWriterManager] = {

    val httpClientBuilder = HttpClient.newBuilder()
      .connectTimeout(Duration.ofMillis(config.timeout.connectionTimeoutMs.toLong))

    val builderAfterSSL =
      convertToScalaOption(unpackOrThrow(config.ssl.toSslContext)).fold(httpClientBuilder)(httpClientBuilder.sslContext)

    val httpClient = builderAfterSSL.build()

    val retriablePolicy: RetryPolicy[IO] = buildRetriablePolicy(sinkName, config)

    val clientResource: Resource[IO, Client[IO]] = JdkHttpClient[IO](httpClient)

    val metricsResetter = new MetricsResetter(metrics, 5.minutes, 30.seconds)
    for {

      (client, cResRel)    <- clientResource.allocated
      (_, resetterRelease) <- metricsResetter.scheduleResetAndUpdate.allocated
      retriableClient       = Retry(retriablePolicy)(client)
      writersRef           <- Ref.of[IO, Map[Topic, HttpWriter]](Map.empty)
      writerCreationLock   <- Semaphore[IO](1)
      sender <- HttpRequestSender(
        sinkName,
        config.method.toHttp4sMethod,
        retriableClient,
        config.authentication,
        metrics,
        config.compression,
      )
      batchPolicy = config.batch.toBatchPolicy

    } yield new HttpWriterManager(
      sinkName,
      template,
      sender,
      batchPolicy,
      //close the resetter first
      resetterRelease.guarantee(cResRel),
      writersRef,
      writerCreationLock,
      terminate,
      config.errorThreshold,
      config.uploadSyncPeriod,
      config.tidyJson,
      config.errorReportingController,
      config.successReportingController,
      config.maxQueueSize,
      config.maxQueueOfferTimeout,
    )
  }

  def buildRetriablePolicy(sinkName: String, config: HttpSinkConfig): RetryPolicy[IO] = {
    val retryPolicy: Int => Option[FiniteDuration] = config.retries match {
      case FixedRetryConfig(maxRetries, intervalMs, _) =>
        logger.info(
          s"[$sinkName] Setting up http client with Fixed retry mode and the max retries is $maxRetries and the interval is $intervalMs ms",
        )
        FixedRetryConfig.fixedInterval(intervalMs.millis, maxRetries)
      case ExponentialRetryConfig(maxRetries, maxTimeoutMs, _) =>
        logger.info(
          s"[$sinkName] Setting up http client with Exponential retry mode and the max retries is $maxRetries and max timeout is $maxTimeoutMs ms",
        )
        RetryPolicy.exponentialBackoff(maxTimeoutMs.millis, maxRetries)
    }

    val retriableFn: Either[Throwable, Response[IO]] => Boolean =
      isErrorOrRetriableStatus(_, config.retries.onStatusCodes.toSet)

    val retriablePolicy = RetryPolicy[IO](
      retryPolicy,
      retriable = (_, response) => retriableFn(response),
    )
    retriablePolicy
  }

  /**
   * Determines if the result is an error or contains a retriable status code.
   *
   * @param result The result to check.
   * @param statusCodes The set of retriable status codes.
   * @tparam F The effect type.
   * @return `true` if the result is an error or contains a retriable status code, `false` otherwise.
   */
  def isErrorOrRetriableStatus[F[_]](result: Either[Throwable, Response[F]], statusCodes: Set[Int]): Boolean =
    result match {
      case Right(resp)                     => statusCodes(resp.status.code)
      case Left(WaitQueueTimeoutException) => false
      case _                               => true
    }
}

/**
 * The `HttpWriterManager` class manages HTTP writers and handles the logic for processing and committing records.
 *
 * @param sinkName The name of the sink.
 * @param template The template type.
 * @param httpRequestSender The HTTP request sender.
 * @param commitPolicy The commit policy.
 * @param close An `IO` action to close the manager.
 * @param writersRef A reference to the map of HTTP writers.
 * @param deferred A deferred value to signal termination.
 * @param errorThreshold The error threshold.
 * @param uploadSyncPeriod The upload synchronization period.
 * @param tidyJson Whether to tidy JSON.
 * @param errorReportingController The error reporting controller.
 * @param successReportingController The success reporting controller.
 * @param t An implicit `Temporal` instance.
 */
class HttpWriterManager(
  sinkName:                   String,
  template:                   TemplateType,
  httpRequestSender:          HttpRequestSender,
  batchPolicy:                BatchPolicy,
  private val close:          IO[Unit],
  writersRef:                 Ref[IO, Map[Topic, HttpWriter]],
  writerCreationLock:         Semaphore[IO],
  deferred:                   Deferred[IO, Either[Throwable, Unit]],
  errorThreshold:             Int,
  uploadSyncPeriod:           Int,
  tidyJson:                   Boolean,
  errorReportingController:   ReportingController[HttpFailureConnectorSpecificRecordData],
  successReportingController: ReportingController[HttpSuccessConnectorSpecificRecordData],
  maxQueueSize:               Int,
  maxQueueOfferTimeout:       FiniteDuration,
)(
  implicit
  t: Temporal[IO],
) extends LazyLogging {

  // The task-level error callback is supplied by `start`. Writers are created lazily (per topic)
  // during `put`, which always runs after `start`, so the callback is available by then.
  private val errCallbackRef: Ref[IO, Throwable => IO[Unit]] =
    Ref.unsafe[IO, Throwable => IO[Unit]]((_: Throwable) => IO.unit)

  // Handles to the per-topic consumer fibers, retained so `awaitConsumers` can wait for them to
  // finish (their in-flight work cancelled) before the HTTP client is released on shutdown.
  private val consumerFibersRef: Ref[IO, List[FiberIO[Unit]]] =
    Ref.unsafe[IO, List[FiberIO[Unit]]](List.empty)

  /**
   * Creates a new HTTP writer for a topic and starts its long-lived consumer fiber. The fiber runs
   * until the manager's termination signal fires; any unrecovered error is forwarded to the task
   * error callback (which fails the next `put`).
   *
   * @return An `IO` action that creates a new `HttpWriter`.
   */
  private def createNewHttpWriter(): IO[HttpWriter] =
    for {
      writer <- HttpWriter.create(
        sinkName             = sinkName,
        sender               = httpRequestSender,
        template             = template,
        batchPolicy          = batchPolicy,
        maxQueueSize         = maxQueueSize,
        maxQueueOfferTimeout = maxQueueOfferTimeout,
        errorThreshold       = errorThreshold,
        tidyJson             = tidyJson,
        errorReporter        = errorReportingController,
        successReporter      = successReportingController,
      )
      errCallback <- errCallbackRef.get
      _           <- startConsumer(writer, errCallback)
    } yield writer

  /**
   * Runs a writer's consumer loop on a background fiber. The loop is raced against the manager's
   * termination `Deferred` so it is cancelled cleanly on `stop`. The fiber handle is retained so
   * `awaitConsumers` can join it during shutdown.
   */
  private def startConsumer(writer: HttpWriter, errCallback: Throwable => IO[Unit]): IO[Unit] =
    IO.race(writer.consume(), deferred.get)
      .void
      .handleErrorWith(e => IO(logger.error(s"[$sinkName] HttpWriter consumer failed", e)) *> errCallback(e))
      .start
      .flatMap(fiber => consumerFibersRef.update(fiber :: _))

  /**
   * Waits for the consumer fibers to finish once termination has been signalled (via `deferred`),
   * so their in-flight requests are cancelled before the shared HTTP client is released. Bounded by
   * a timeout so a stuck request cannot make shutdown hang indefinitely.
   */
  private def awaitConsumers: IO[Unit] =
    consumerFibersRef.get
      .flatMap(fibers => fibers.traverse_(_.join.void))
      .timeoutTo(
        30.seconds,
        IO(logger.warn(s"[$sinkName] Timed out waiting for consumer fibers to stop")),
      )

  /**
   * Closes the reporting controllers. Must only run once the consumer fibers have stopped:
   * `HttpWriter.reportResult` enqueues onto these controllers from an in-flight send, and those
   * reports are dropped (and the offer can block on a queue nothing drains) if they are closed first.
   */
  private def closeReportingControllers: IO[Unit] =
    IO(errorReportingController.close()).guarantee(IO(successReportingController.close()))

  /**
   * Orderly teardown, to be run after the termination signal has been completed. Each stage is a
   * finalizer of the previous one, so a failure or timeout part-way still releases everything:
   * wait for the consumer fibers (their in-flight requests cancelled), only then stop the
   * reporting controllers (so no in-flight send can enqueue into a closed reporter), and finally
   * release the shared HTTP client.
   */
  def shutdown: IO[Unit] =
    awaitConsumers.guarantee(closeReportingControllers).guarantee(close)

  /**
   * Gets or creates an HTTP writer for the given topic.
   *
   * @param topic The topic for which to get or create the writer.
   * @return An `IO` action that returns the `HttpWriter`.
   */
  def getWriter(topic: Topic): IO[HttpWriter] =
    writersRef.get.flatMap { writers =>
      writers.get(topic) match {
        case Some(value) => IO.pure(value)
        case None        =>
          // Serialise creation so only one consumer fiber is ever started per topic: a lost
          // optimistic update would otherwise leak a running fiber and strand records in a writer
          // that is not in the map. The lock is only engaged on a cache miss (rare).
          writerCreationLock.permit.use { _ =>
            writersRef.get.flatMap { latest =>
              latest.get(topic) match {
                case Some(value) => IO.pure(value)
                case None => for {
                    newWriter <- createNewHttpWriter()
                    _         <- writersRef.update(_ + (topic -> newWriter))
                  } yield newWriter
              }
            }
          }
      }
    }

  /**
   * Resets the per-partition dedup high-water mark for the given partitions so that records
   * redelivered after a rebalance (Kafka rewinds the consumer to the last committed offset) are not
   * silently discarded as duplicates. Partitions whose topic has no writer yet need nothing: a
   * freshly created writer starts with an empty dedup map.
   */
  def onPartitionsOpened(partitions: Set[TopicPartition]): IO[Unit] =
    if (partitions.isEmpty) IO.unit
    else
      writersRef.get.flatMap { writers =>
        partitions.groupBy(_.topic).toList.traverse_ {
          case (topic, topicPartitions) =>
            writers.get(topic) match {
              case Some(writer) => writer.resetAcceptedOffsets(topicPartitions)
              case None         => IO.unit
            }
        }
      }

  /**
   * Pre-commits the current offsets.
   * (answers the question: what have you committed?)
   *
   * @param currentOffsets The current offsets.
   * @return An `IO` action that returns the pre-committed offsets.
   */
  def preCommit(currentOffsets: Map[TopicPartition, OffsetAndMetadata]): IO[Map[TopicPartition, OffsetAndMetadata]] = {

    val currentOffsetsGroupedIO: IO[Map[Topic, Map[TopicPartition, OffsetAndMetadata]]] = IO
      .pure(currentOffsets)
      .map(_.groupBy {
        case (TopicPartition(topic, _), _) => topic
      })

    for {
      curr    <- currentOffsetsGroupedIO
      writers <- writersRef.get
      res <- writers.toList.traverse {
        case (topic, writer) =>
          writer.preCommit(curr(topic))
      }.map(_.flatten.toMap)
    } yield res

  }

  /**
   * Starts the `HttpWriterManager`.
   *
   * @param errCallback The error callback.
   * @return An `IO` action that starts the manager.
   */
  def start(errCallback: Throwable => IO[Unit]): IO[Unit] =
    for {
      _ <- errCallbackRef.set(errCallback)
      _ <- IO(
        logger.info(
          s"[$sinkName] starting HttpWriterManager (per-topic consumer fibers; " +
            s"'${io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef.UploadSyncPeriodProp}'=$uploadSyncPeriod is deprecated and ignored)",
        ),
      )
    } yield ()
}
