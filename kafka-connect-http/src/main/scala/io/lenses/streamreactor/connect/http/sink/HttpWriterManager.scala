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
import cats.effect.std.Mutex
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.util.EitherUtils.unpackOrThrow
import io.lenses.streamreactor.common.utils.CyclopsToScalaOption.convertToScalaOption
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.http.sink.client.HttpRequestSender
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitPolicy
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
import scala.collection.mutable
import scala.concurrent.duration.DurationInt

object HttpWriterManager extends StrictLogging {
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
    val httpWritersMap = mutable.Map[Topic, HttpWriter]()

    for {
      (client, cResRel) <- clientResource.allocated
      retriableClient    = Retry(retriablePolicy)(client)
      sender <- HttpRequestSender(
        sinkName,
        config.method.toHttp4sMethod,
        retriableClient,
        config.authentication,
      )
      commitPolicy = config.batch.toCommitPolicy
      writersLock <- Mutex[IO]
    } yield new HttpWriterManager(
      sinkName,
      template,
      sender,
      if (commitPolicy.conditions.nonEmpty) commitPolicy else HttpCommitPolicy.Default,
      cResRel,
      httpWritersMap,
      writersLock,
      terminate,
      config.errorThreshold,
      config.uploadSyncPeriod,
      config.tidyJson,
      config.errorReportingController,
      config.successReportingController,
    )
  }

  /*
    Returns true if parameter is a Left or if the response contains a retriable status(as per HTTP spec)
   */
  def isErrorOrRetriableStatus[F[_]](result: Either[Throwable, Response[F]], statusCodes: Set[Int]): Boolean =
    result match {
      case Right(resp)                     => statusCodes(resp.status.code)
      case Left(WaitQueueTimeoutException) => false
      case _                               => true
    }
}
class HttpWriterManager(
  sinkName:                   String,
  template:                   TemplateType,
  httpRequestSender:          HttpRequestSender,
  commitPolicy:               CommitPolicy,
  val close:                  IO[Unit],
  writers:                    mutable.Map[Topic, HttpWriter],
  writersLock:                Mutex[IO],
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
  private def createNewHttpWriter(): IO[HttpWriter] =
    for {
      commitPolicy     <- IO.pure(commitPolicy)
      semaphore        <- Mutex[IO]
      commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default(sinkName))
    } yield new HttpWriter(
      sinkName = sinkName,
      sender   = httpRequestSender,
      template = template,
      recordsQueue = new RecordsQueue(
        mutable.Queue[RenderedRecord](),
        commitPolicy,
        () => commitContextRef.get.unsafeRunSync(),
      ),
      commitContextRef = commitContextRef,
      errorThreshold   = errorThreshold,
      tidyJson         = tidyJson,
      errorReporter    = errorReportingController,
      successReporter  = successReportingController,
      semaphore,
    )

  def closeReportingControllers(): Unit = {
    errorReportingController.close()
    successReportingController.close()
  }

  def getWriter(topic: Topic): IO[HttpWriter] =
    writersLock.lock.surround {
      writers.get(topic) match {
        case Some(writer) => IO.pure(writer)
        case None =>
          for {
            newWriter <- createNewHttpWriter()
            _          = writers.put(topic, newWriter)
          } yield newWriter
      }
    }

  // answers the question: what have you committed?
  def preCommit(currentOffsets: Map[TopicPartition, OffsetAndMetadata]): IO[Map[TopicPartition, OffsetAndMetadata]] = {

    val currentOffsetsGroupedIO: IO[Map[Topic, Map[TopicPartition, OffsetAndMetadata]]] = IO
      .pure(currentOffsets)
      .map(_.groupBy {
        case (TopicPartition(topic, _), _) => topic
      })

    writersLock.lock.surround {
      for {
        curr <- currentOffsetsGroupedIO
        res <- writers.toList.traverse {
          case (topic, writer) =>
            writer.preCommit(curr(topic))
        }.map(_.flatten.toMap)
      } yield res
    }
  }

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

  private def process(): IO[List[Either[Throwable, Unit]]] =
    for {
      _ <- IO.delay(logger.trace(s"[$sinkName] WriterManager.process()"))
      fiberIOs <- writersLock.lock.surround {
        for {
          _ <- IO.whenA(writers.isEmpty) {
            IO.delay(logger.info(
              s"[$sinkName] HttpWriterManager has no writers. Perhaps no records have been put to the sink yet.",
            ))
          }
          fiberIOs = writers.toList.map {
            case (id, writer) =>
              IO.delay(logger.trace(s"[$sinkName] starting process for writer $id")) *> writer.process().attempt.start
          }
        } yield fiberIOs
      }
      fibers <- fiberIOs.sequence
      results <- fibers.traverse { fiber =>
        fiber.join.flatMap {
          case Outcome.Succeeded(io) => io
          case Outcome.Errored(e)    => IO.pure(Left(e))
          case Outcome.Canceled()    => IO.raiseError(new RuntimeException("IO canceled"))
        }
      }
    } yield results

}
