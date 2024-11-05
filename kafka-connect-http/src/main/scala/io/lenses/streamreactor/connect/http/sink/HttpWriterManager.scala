/*
 * Copyright 2017-2024 Lenses.io Ltd
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

import cats.effect.IO
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.kernel.Deferred
import cats.effect.kernel.Outcome
import cats.effect.kernel.Temporal
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.util.EitherUtils.unpackOrThrow
import io.lenses.streamreactor.common.utils.CyclopsToScalaOption.convertToScalaOption
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.http.sink.client.HttpRequestSender
import io.lenses.streamreactor.connect.http.sink.commit.BatchPolicy
import io.lenses.streamreactor.connect.http.sink.commit.HttpBatchPolicy
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfig
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpFailureConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpSuccessConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
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
import scala.collection.immutable.Queue
import scala.concurrent.duration.DurationInt

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
  )(
    implicit
    t: Temporal[IO],
  ): IO[HttpWriterManager] = {

    val httpClientBuilder = HttpClient.newBuilder()
      .connectTimeout(Duration.ofMillis(config.timeout.connectionTimeoutMs.toLong))

    val builderAfterSSL =
      convertToScalaOption(unpackOrThrow(config.ssl.toSslContext)).fold(httpClientBuilder)(httpClientBuilder.sslContext)

    val httpClient = builderAfterSSL.build()
    logger.info(
      s"[$sinkName] Setting up http client with maxTimeout: ${config.retries.maxTimeoutMs.millis}, retries: ${config.retries.maxRetries}",
    )
    val retryPolicy = RetryPolicy.exponentialBackoff(config.retries.maxTimeoutMs.millis, config.retries.maxRetries)

    val retriableFn: Either[Throwable, Response[IO]] => Boolean =
      isErrorOrRetriableStatus(_, config.retries.onStatusCodes.toSet)

    val retriablePolicy = RetryPolicy[IO](
      retryPolicy,
      retriable = (_, response) => retriableFn(response),
    )

    val clientResource: Resource[IO, Client[IO]] = JdkHttpClient[IO](httpClient)

    for {
      (client, cResRel) <- clientResource.allocated
      retriableClient    = Retry(retriablePolicy)(client)
      writersRef        <- Ref.of[IO, Map[Topic, HttpWriter]](Map.empty)
      sender <- HttpRequestSender(
        sinkName,
        config.method.toHttp4sMethod,
        retriableClient,
        config.authentication,
      )
      batchPolicy = config.batch.toBatchPolicy

    } yield new HttpWriterManager(
      sinkName,
      template,
      sender,
      if (batchPolicy.conditions.nonEmpty) batchPolicy else HttpBatchPolicy.Default,
      cResRel,
      writersRef,
      terminate,
      config.errorThreshold,
      config.uploadSyncPeriod,
      config.tidyJson,
      config.errorReportingController,
      config.successReportingController,
    )
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
  val close:                  IO[Unit],
  writersRef:                 Ref[IO, Map[Topic, HttpWriter]],
  deferred:                   Deferred[IO, Either[Throwable, Unit]],
  errorThreshold:             Int,
  uploadSyncPeriod:           Int,
  tidyJson:                   Boolean,
  errorReportingController:   ReportingController[HttpFailureConnectorSpecificRecordData],
  successReportingController: ReportingController[HttpSuccessConnectorSpecificRecordData],
)(
  implicit
  t: Temporal[IO],
) extends LazyLogging {

  /**
    * Creates a new HTTP writer.
    *
    * @return An `IO` action that creates a new `HttpWriter`.
    */
  private def createNewHttpWriter(): IO[HttpWriter] =
    for {
      batchPolicy      <- IO.pure(batchPolicy)
      recordsQueueRef  <- Ref.of[IO, Queue[RenderedRecord]](Queue.empty)
      commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default(sinkName))
    } yield new HttpWriter(
      sinkName = sinkName,
      sender   = httpRequestSender,
      template = template,
      recordsQueue =
        new RecordsQueue(recordsQueueRef, commitContextRef, batchPolicy),
      errorThreshold   = errorThreshold,
      tidyJson         = tidyJson,
      errorReporter    = errorReportingController,
      successReporter  = successReportingController,
      commitContextRef = commitContextRef,
    )

  /**
    * Closes the reporting controllers.
    */
  def closeReportingControllers(): Unit = {
    errorReportingController.close()
    successReportingController.close()
  }

  /**
    * Gets or creates an HTTP writer for the given topic.
    *
    * @param topic The topic for which to get or create the writer.
    * @return An `IO` action that returns the `HttpWriter`.
    */
  def getWriter(topic: Topic): IO[HttpWriter] =
    writersRef.access.flatMap {
      case (currentValue, updater) =>
        currentValue.get(topic) match {
          case Some(value) => IO.pure(value)
          case None => for {
              newWriter <- createNewHttpWriter()
              _         <- updater(currentValue + (topic -> newWriter))
            } yield newWriter
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
  def start(errCallback: Throwable => IO[Unit]): IO[Unit] = {
    import scala.concurrent.duration._
    for {
      _ <- IO(logger.info(s"[$sinkName] starting HttpWriterManager"))
      _ <- fs2
        .Stream
        .fixedRate(uploadSyncPeriod.millis)
        .evalMap(_ => process().flatMap(handleResult(_, errCallback)).void)
        .interruptWhen(deferred)
        .onComplete(fs2.Stream.eval(close))
        .compile
        .drain
        .background
        .allocated
    } yield ()
  }

  /**
    * Handles the result of the writer processes.
    *
    * @param writersResult The result of the writer processes.
    * @param errCallback The error callback.
    * @return An `IO` action that handles the result.
    */
  private def handleResult(
    writersResult: List[Either[Throwable, _]],
    errCallback:   Throwable => IO[Unit],
  ): IO[Unit] =
    for {
      failures <- IO(writersResult.collect {
        case Left(error: Throwable) => error
      })
      _ <- if (failures.nonEmpty) {
        logger.error(s"[$sinkName] Some writer processes failed: $failures")
        failures.traverse(errCallback)
      } else {
        logger.debug(s"[$sinkName] All writer processes completed successfully")
        IO.unit
      }
    } yield ()

  /**
    * Processes the writers.
    *
    * @return An `IO` action that processes the writers.
    */
  private def process(): IO[List[Either[Throwable, Unit]]] =
    for {
      // Log the start of the processing
      _ <- IO.delay(logger.trace(s"[$sinkName] WriterManager.process()"))

      // Retrieve the current writers
      writers <- writersRef.get

      // Log if there are no writers
      _ <- IO.whenA(writers.isEmpty) {
        IO.delay(
          logger.info(
            s"[$sinkName] HttpWriterManager has no writers. " +
              "Perhaps no records have been put to the sink yet.",
          ),
        )
      }

      // Create a list of fiber-starting IO operations for each writer
      fiberIOs = writers.toList.map {
        case (id, writer) =>
          IO.delay(logger.trace(s"[$sinkName] Starting process for writer $id")) *>
            writer.process().attempt.start
      }

      // Execute all fiber-starting IO operations sequentially
      fibers <- fiberIOs.sequence

      // Collect the results from all fibers
      results <- fibers.traverse { fiber =>
        fiber.join.flatMap {
          case Outcome.Succeeded(io) => io
          case Outcome.Errored(e)    => IO.pure(Left(e))
          case Outcome.Canceled()    => IO.pure(Left(new RuntimeException("IO canceled")))
        }
      }
    } yield results
}
