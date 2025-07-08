/*
 * Copyright 2017-2025 Lenses.io Ltd
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
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.http.sink.client.HttpRequestSender
import io.lenses.streamreactor.connect.http.sink.client.HttpResponseFailure
import io.lenses.streamreactor.connect.http.sink.client.HttpResponseSuccess
import io.lenses.streamreactor.common.batch.HttpCommitContext
import io.lenses.streamreactor.common.batch.BatchInfo
import io.lenses.streamreactor.common.batch.EmptyBatchInfo
import io.lenses.streamreactor.common.batch.NonEmptyBatchInfo
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

class HttpWriterTest extends AsyncIOSpec with AsyncFunSuiteLike with Matchers with MockitoSugar {

  private val sinkName  = "MySinkName"
  private val timestamp = 125L

  private val topicPartition: TopicPartition    = Topic("myTopic").withPartition(1)
  private val defaultContext: HttpCommitContext = HttpCommitContext.default("My Sink")

  private val record1      = RenderedRecord(topicPartition.atOffset(100), timestamp, "record1", Seq.empty, "")
  private val record2      = RenderedRecord(topicPartition.atOffset(101), timestamp, "record2", Seq.empty, "")
  private val recordsToAdd = NonEmptySeq.of(record1, record2)

  private val senderMock   = mock[HttpRequestSender]
  private val templateMock = mock[TemplateType]

  test("add method should add records to the queue") {

    val batchInfo    = mock[BatchInfo]
    val recordsQueue = mockRecordQueue(batchInfo)

    {
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default("My Sink"))
        httpWriter = new HttpWriter(sinkName,
                                    senderMock,
                                    templateMock,
                                    recordsQueue,
                                    5,
                                    false,
                                    mock[ReportingController[HttpFailureConnectorSpecificRecordData]],
                                    mock[ReportingController[HttpSuccessConnectorSpecificRecordData]],
                                    commitContextRef,
        )

        _ <- httpWriter.add(recordsToAdd)
      } yield recordsToAdd
    } asserting { _ =>
      verify(recordsQueue).enqueueAll(recordsToAdd)
      succeed
    }
  }

  test("process method should flush records when the queue is non-empty and commit policy requires flush") {
    when(senderMock.sendHttpRequest(any[ProcessedTemplate])).thenReturn(IO(HttpResponseSuccess(200, "OK".some).asRight))

    when(templateMock.process(eqTo(recordsToAdd), eqTo(false))).thenReturn(Right(ProcessedTemplate(
      "a",
      "b",
      Seq.empty,
    )))

    val batchInfo = NonEmptyBatchInfo(
      recordsToAdd,
      defaultContext,
      100,
    )

    val recordsQueue = mockRecordQueue(batchInfo)

    {

      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](defaultContext)
        httpWriter = new HttpWriter(sinkName,
                                    senderMock,
                                    templateMock,
                                    recordsQueue,
                                    5,
                                    false,
                                    mock[ReportingController[HttpFailureConnectorSpecificRecordData]],
                                    mock[ReportingController[HttpSuccessConnectorSpecificRecordData]],
                                    commitContextRef,
        )

        _              <- httpWriter.process()
        updatedContext <- commitContextRef.get
      } yield updatedContext
    }.asserting {
      updatedContext =>
        verify(recordsQueue).dequeue(recordsToAdd)
        updatedContext should not be defaultContext
    }
  }

  test("process method should not flush records when the queue is empty") {

    when(senderMock.sendHttpRequest(any[ProcessedTemplate])).thenReturn(IO(HttpResponseFailure("fail",
                                                                                               none,
                                                                                               404.some,
                                                                                               none,
    ).asLeft))

    val recordsQueue: RecordsQueue = mockRecordQueue(EmptyBatchInfo(0))

    val templateMock = mock[TemplateType]

    {
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](defaultContext)
        httpWriter = new HttpWriter(sinkName,
                                    senderMock,
                                    templateMock,
                                    recordsQueue,
                                    5,
                                    false,
                                    mock[ReportingController[HttpFailureConnectorSpecificRecordData]],
                                    mock[ReportingController[HttpSuccessConnectorSpecificRecordData]],
                                    commitContextRef,
        )

        _              <- httpWriter.process()
        updatedContext <- commitContextRef.get
      } yield {
        updatedContext
      }
    }.asserting {
      updatedContext =>
        updatedContext shouldBe defaultContext
    }
  }

  private def mockRecordQueue(batchInfo: BatchInfo) = {
    val recordsQueue = mock[RecordsQueue]
    when(recordsQueue.popBatch()).thenReturn(IO(batchInfo))
    when(recordsQueue.enqueueAll(recordsToAdd)).thenReturn(IO.unit)
    recordsQueue
  }
}
