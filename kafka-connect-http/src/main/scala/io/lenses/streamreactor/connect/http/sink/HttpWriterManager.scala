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

import cats.effect.FiberIO
import cats.effect.IO
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.kernel.Deferred
import cats.effect.kernel.Outcome
import cats.effect.kernel.Temporal
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
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
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import io.lenses.streamreactor.connect.http.sink.tpl.TemplateType
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

object HttpWriterManager extends StrictLogging {
  def apply(
    sinkName:  String,
    config:    HttpSinkConfig,
    template:  TemplateType,
    terminate: Deferred[IO, Either[Throwable, Unit]],
  )(
    implicit
    t: Temporal[IO],
  ): HttpWriterManager = {

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
    // This code would need to propagate the resource up to the caller
    clientResource.allocated.unsafeRunSync() match {
      case (client, cResRel) =>
        val retriableClient = Retry(retriablePolicy)(client)
        val requestSender = new HttpRequestSender(
          sinkName,
          config.authentication,
          config.method.toHttp4sMethod,
          retriableClient,
        )
        val commitPolicy = config.batch.toCommitPolicy
        new HttpWriterManager(
          sinkName,
          template,
          requestSender,
          if (commitPolicy.conditions.nonEmpty) commitPolicy else HttpCommitPolicy.Default,
          cResRel,
          Ref.unsafe(Map[Topic, HttpWriter]()),
          terminate,
          config.errorThreshold,
          config.uploadSyncPeriod,
        )
    }

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
  sinkName:          String,
  template:          TemplateType,
  httpRequestSender: HttpRequestSender,
  commitPolicy:      CommitPolicy,
  val close:         IO[Unit],
  writersRef:        Ref[IO, Map[Topic, HttpWriter]],
  deferred:          Deferred[IO, Either[Throwable, Unit]],
  errorThreshold:    Int,
  uploadSyncPeriod:  Int,
)(
  implicit
  t: Temporal[IO],
) extends LazyLogging {

  private def createNewHttpWriter(): HttpWriter =
    new HttpWriter(
      sinkName     = sinkName,
      commitPolicy = commitPolicy,
      sender       = httpRequestSender,
      template     = template,
      Ref.unsafe[IO, Queue[RenderedRecord]](Queue()),
      Ref.unsafe[IO, HttpCommitContext](HttpCommitContext.default(sinkName)),
      errorThreshold,
    )

  def getWriter(topic: Topic): IO[HttpWriter] = {
    var foundWriter = Option.empty[HttpWriter]
    for {
      _ <- writersRef.getAndUpdate {
        writers =>
          foundWriter = writers.get(topic)
          if (foundWriter.nonEmpty) {
            writers // no update
          } else {
            val newWriter = createNewHttpWriter()
            foundWriter = newWriter.some
            writers + (topic -> newWriter)
          }
      }
      o <- IO.fromOption(foundWriter)(new IllegalStateException("No writer found"))
    } yield o
  }

  // answers the question: what have you committed?
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

  def start(errCallback: Throwable => Unit): IO[Unit] = {
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
    errCallback:   Throwable => Unit,
  ): IO[Unit] = IO {
    // Handle the result of individual writer processes
    val failures = writersResult.collect {
      case Left(error: Throwable) => error
    }
    if (failures.nonEmpty) {
      logger.error(s"[$sinkName] Some writer processes failed: $failures")
      failures.foreach(wr => errCallback(wr))
    } else {
      logger.debug(s"[$sinkName] All writer processes completed successfully")
    }
  }

  def process(): IO[List[Either[Throwable, Unit]]] = {
    logger.trace(s"[$sinkName] WriterManager.process()")
    writersRef.get.flatMap { writersMap =>
      if (writersMap.isEmpty) {
        logger.info(s"[$sinkName] HttpWriterManager has no writers.  Perhaps no records have been put to the sink yet.")
      }

      // Create an IO action for each writer to process it in parallel
      val fiberIOs: List[IO[FiberIO[_]]] = writersMap.map {
        case (id, writer) =>
          logger.trace(s"[$sinkName] starting process for writer $id")
          writer.process().start
      }.toList

      // Return a list of Fibers
      fiberIOs.traverse { e =>
        val f = e.flatMap(_.join.attempt).flatMap {
          case Left(value: Throwable) => IO.pure(Left(value))
          case Right(value: Outcome[IO, Throwable, _]) => value match {
              case Outcome.Succeeded(_)   => IO.pure(Right(()))
              case Outcome.Errored(error) => IO.pure(Left(error))
              case Outcome.Canceled()     => IO.raiseError(new RuntimeException("IO canceled"))
            }
        }
        f
      }

    }
  }

}
