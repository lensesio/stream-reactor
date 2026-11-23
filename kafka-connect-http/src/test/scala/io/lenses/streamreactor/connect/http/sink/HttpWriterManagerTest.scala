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
package io.lenses.streamreactor.connect.http.sink;

import cats.data.NonEmptySeq
import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxEitherId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.batch.BatchPolicy
import io.lenses.streamreactor.common.batch.Count
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.http.sink.client.HttpRequestSender
import io.lenses.streamreactor.connect.http.sink.client.HttpResponseFailure
import io.lenses.streamreactor.connect.http.sink.client.HttpResponseSuccess
import io.lenses.streamreactor.connect.http.sink.config.ErrorNullPayloadHandler
import io.lenses.streamreactor.connect.http.sink.metrics.HttpSinkMetrics
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpFailureConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpSuccessConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.tpl.Headers
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import io.lenses.streamreactor.connect.http.sink.tpl.SimpleTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.TemplateType
import io.lenses.streamreactor.connect.reporting.ReportingController
import org.http4s.Response
import org.http4s.Status
import org.http4s.WaitQueueTimeoutException
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.mockito.invocation.InvocationOnMock
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class HttpWriterManagerTest extends AnyFunSuiteLike with Matchers with EitherValues with MockitoSugar with LazyLogging {

  test("isErrorOrStatus returns true if the statusCodes is matched") {
    val statusCodes = Set(408, 429)

    HttpWriterManager.isErrorOrRetriableStatus(Right(Response(Status.RequestTimeout)), statusCodes) should be(true)
    HttpWriterManager.isErrorOrRetriableStatus(Right(Response(Status.TooManyRequests)), statusCodes) should be(true)
  }
  test("isErrorOrStatus returns false if the statusCodes is not matched") {
    val statusCodes = Set(408, 429)

    HttpWriterManager.isErrorOrRetriableStatus(Right(Response(Status.Ok)), statusCodes) should be(false)
    HttpWriterManager.isErrorOrRetriableStatus(Right(Response(Status.Created)), statusCodes) should be(false)
  }
  test("isErrorOrStatus returns true if the result is not a Right") {
    val statusCodes = Set(408, 429)

    HttpWriterManager.isErrorOrRetriableStatus(Left(new Exception("")), statusCodes) should be(true)
  }
  test("isErrorOrStatus returns false if the exception is WaitQueueTimeoutException") {
    val statusCodes = Set(408, 429)

    HttpWriterManager.isErrorOrRetriableStatus(Left(WaitQueueTimeoutException), statusCodes) should be(false)
  }

  // Covers the PR-review finding: `HttpSinkTask.stop()` used to close the reporting controllers
  // before waiting for the consumer fibers, so an in-flight `sendBatch` could still reach
  // `reportResult` and enqueue onto a controller that was already shutting down. `shutdown` now owns
  // the whole teardown order, so these tests exercise it directly instead of `HttpSinkTask`.

  private val sinkName = "MySinkName"
  private val topic    = Topic("myTopic")
  private val record   = RenderedRecord(topic.withPartition(1).atOffset(1L), 1L, "record", Seq.empty, "")

  private def successTemplate(): TemplateType =
    SimpleTemplate("http://bench.invalid",
                   "content",
                   Headers(Seq.empty, copyMessageHeaders = false),
                   ErrorNullPayloadHandler,
    )

  private def buildManager(
    sender:            HttpRequestSender,
    errorController:   ReportingController[HttpFailureConnectorSpecificRecordData],
    successController: ReportingController[HttpSuccessConnectorSpecificRecordData],
    closeIO:           IO[Unit],
    deferred:          Deferred[IO, Either[Throwable, Unit]],
  ): IO[HttpWriterManager] =
    for {
      writersRef <- Ref.of[IO, Map[Topic, HttpWriter]](Map.empty)
      lock       <- Semaphore[IO](1)
    } yield new HttpWriterManager(
      sinkName                   = sinkName,
      template                   = successTemplate(),
      httpRequestSender          = sender,
      batchPolicy                = BatchPolicy(logger, Count(1)),
      close                      = closeIO,
      writersRef                 = writersRef,
      writerCreationLock         = lock,
      deferred                   = deferred,
      errorThreshold             = 5,
      uploadSyncPeriod           = 0,
      tidyJson                   = false,
      errorReportingController   = errorController,
      successReportingController = successController,
      maxQueueSize               = 10,
      maxQueueOfferTimeout       = 1.minute,
      metrics                    = new HttpSinkMetrics(),
    )

  test("shutdown does not close the reporting controllers until it is actually run") {
    val errorController   = mock[ReportingController[HttpFailureConnectorSpecificRecordData]]
    val successController = mock[ReportingController[HttpSuccessConnectorSpecificRecordData]]

    val io = for {
      deferred <- Deferred[IO, Either[Throwable, Unit]]
      manager  <- buildManager(mock[HttpRequestSender], errorController, successController, IO.unit, deferred)
      _        <- IO(verify(errorController, never).close())
      _        <- IO(verify(successController, never).close())
      _        <- deferred.complete(().asRight)
      _        <- manager.shutdown
    } yield {
      verify(errorController).close()
      verify(successController).close()
    }

    io.unsafeRunSync()
  }

  test("shutdown closes the success controller and releases the client even when the error controller's close throws") {
    val errorController = mock[ReportingController[HttpFailureConnectorSpecificRecordData]]
    when(errorController.close()).thenThrow(new RuntimeException("boom"))
    val successController = mock[ReportingController[HttpSuccessConnectorSpecificRecordData]]
    val closed            = new ConcurrentLinkedQueue[String]()

    val io = for {
      deferred <- Deferred[IO, Either[Throwable, Unit]]
      manager <- buildManager(mock[HttpRequestSender],
                              errorController,
                              successController,
                              IO(closed.add("close")).void,
                              deferred,
      )
      _      <- deferred.complete(().asRight)
      result <- manager.shutdown.attempt
    } yield {
      result.left.value.getMessage shouldBe "boom"
      verify(successController).close()
      closed.asScala.toList shouldBe List("close")
    }

    io.unsafeRunSync()
  }

  test(
    "shutdown waits for the consumer fiber's in-flight send to be cancelled before closing the reporters and the client",
  ) {
    val log             = new ConcurrentLinkedQueue[String]()
    val errorController = mock[ReportingController[HttpFailureConnectorSpecificRecordData]]
    when(errorController.close()).thenAnswer { (_: InvocationOnMock) => log.add("errorClose"); () }
    val successController = mock[ReportingController[HttpSuccessConnectorSpecificRecordData]]
    when(successController.close()).thenAnswer { (_: InvocationOnMock) => log.add("successClose"); () }
    val sender = mock[HttpRequestSender]

    val io = for {
      entered  <- Deferred[IO, Unit]
      deferred <- Deferred[IO, Either[Throwable, Unit]]
      // The send never completes on its own: it is only ever unblocked by the cancellation that
      // `HttpWriterManager.startConsumer`'s `IO.race(consumer, deferred.get)` delivers once
      // `deferred` is completed, mirroring the real shutdown path exactly.
      _ <- IO {
        when(sender.sendHttpRequest(any[ProcessedTemplate])).thenReturn(
          entered.complete(()) *> IO.never[Either[HttpResponseFailure, HttpResponseSuccess]]
            .onCancel(IO(log.add("consumerCancelled")).void),
        )
      }
      manager <- buildManager(sender, errorController, successController, IO(log.add("close")).void, deferred)
      writer  <- manager.getWriter(topic)
      _       <- writer.add(NonEmptySeq.of(record))
      // The batch is now stuck mid-send, holding its permit.
      _ <- entered.get
      _ <- deferred.complete(().asRight)
      _ <- manager.shutdown
    } yield log.asScala.toList shouldBe List("consumerCancelled", "errorClose", "successClose", "close")

    io.unsafeRunSync()
  }
}
