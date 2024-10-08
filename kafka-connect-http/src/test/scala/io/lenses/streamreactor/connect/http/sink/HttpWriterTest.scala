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
import cats.effect.std.Semaphore
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Count
import io.lenses.streamreactor.connect.http.sink.client.HttpRequestSender
import io.lenses.streamreactor.connect.http.sink.client.HttpResponseFailure
import io.lenses.streamreactor.connect.http.sink.client.HttpResponseSuccess
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpFailureConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpSuccessConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import io.lenses.streamreactor.connect.http.sink.tpl.TemplateType
import io.lenses.streamreactor.connect.reporting.ReportingController
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchersSugar.eqTo
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AsyncFunSuiteLike
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.Queue
import scala.collection.mutable

class HttpWriterTest extends AsyncIOSpec with AsyncFunSuiteLike with Matchers with MockitoSugar {

  private val sinkName  = "MySinkName"
  private val TIMESTAMP = 125L

  private val topicPartition: TopicPartition    = Topic("myTopic").withPartition(1)
  private val defaultContext: HttpCommitContext = HttpCommitContext.default("My Sink")

  test("add method should add records to the queue") {
    val commitPolicy = CommitPolicy(Count(2L))
    val senderMock   = mock[HttpRequestSender]
    val templateMock = mock[TemplateType]
    val recordsQueue = mutable.Queue[RenderedRecord]()
    val recordsToAdd = Seq(
      RenderedRecord(topicPartition.atOffset(100), TIMESTAMP, "record1", Seq.empty, None),
      RenderedRecord(topicPartition.atOffset(101), TIMESTAMP, "record2", Seq.empty, None),
    )

    {
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default("My Sink"))
        queueLock        <- Semaphore[IO](1)
        httpWriter = new HttpWriter(sinkName,
                                    commitPolicy,
                                    senderMock,
                                    templateMock,
                                    recordsQueue,
                                    commitContextRef,
                                    5,
                                    false,
                                    mock[ReportingController[HttpFailureConnectorSpecificRecordData]],
                                    mock[ReportingController[HttpSuccessConnectorSpecificRecordData]],
                                    queueLock,
        )

        _ <- httpWriter.add(recordsToAdd)
      } yield recordsToAdd
    } asserting { _ =>
      recordsQueue shouldBe Queue(recordsToAdd: _*)
    }
  }

  test("process method should flush records when the queue is non-empty and commit policy requires flush") {
    val commitPolicy = CommitPolicy(Count(2L))
    val senderMock   = mock[HttpRequestSender]
    when(senderMock.sendHttpRequest(any[ProcessedTemplate])).thenReturn(IO(HttpResponseSuccess(200, "OK".some).asRight))

    val templateMock = mock[TemplateType]
    when(templateMock.process(any[Seq[RenderedRecord]], eqTo(false))).thenReturn(Right(ProcessedTemplate("a",
                                                                                                         "b",
                                                                                                         Seq.empty,
    )))

    val recordsQueue = new mutable.Queue[RenderedRecord]()

    val recordsToAdd = Seq(
      RenderedRecord(topicPartition.atOffset(100), TIMESTAMP, "record1", Seq.empty, None),
      RenderedRecord(topicPartition.atOffset(101), TIMESTAMP, "record2", Seq.empty, None),
    )

    {
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](defaultContext)
        queueLock        <- Semaphore[IO](1)
        httpWriter = new HttpWriter(sinkName,
                                    commitPolicy,
                                    senderMock,
                                    templateMock,
                                    recordsQueue,
                                    commitContextRef,
                                    5,
                                    false,
                                    mock[ReportingController[HttpFailureConnectorSpecificRecordData]],
                                    mock[ReportingController[HttpSuccessConnectorSpecificRecordData]],
                                    queueLock,
        )

        _              <- httpWriter.add(recordsToAdd)
        _              <- httpWriter.process()
        updatedContext <- commitContextRef.get
      } yield updatedContext
    }.asserting {
      updatedContext =>
        updatedContext should not be defaultContext
        recordsQueue shouldBe empty
    }
  }

  test("process method should not flush records when the queue is empty") {
    val commitPolicy = CommitPolicy(Count(2L))
    val senderMock   = mock[HttpRequestSender]
    when(senderMock.sendHttpRequest(any[ProcessedTemplate])).thenReturn(IO(HttpResponseFailure("fail",
                                                                                               none,
                                                                                               404.some,
                                                                                               none,
    ).asLeft))
    val recordsQueue = mutable.Queue[RenderedRecord]()

    val templateMock = mock[TemplateType]

    for {
      commitContextRef <- Ref.of[IO, HttpCommitContext](defaultContext)
      queueLock        <- Semaphore[IO](1)

      httpWriter = new HttpWriter(sinkName,
                                  commitPolicy,
                                  senderMock,
                                  templateMock,
                                  recordsQueue,
                                  commitContextRef,
                                  5,
                                  false,
                                  mock[ReportingController[HttpFailureConnectorSpecificRecordData]],
                                  mock[ReportingController[HttpSuccessConnectorSpecificRecordData]],
                                  queueLock,
      )

      _              <- httpWriter.process()
      updatedContext <- commitContextRef.get
    } yield {
      updatedContext shouldBe defaultContext
      recordsQueue shouldBe empty
    }
  }
}
