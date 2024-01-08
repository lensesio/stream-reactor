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
import cats.effect.testing.scalatest.AsyncIOSpec
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Count
import io.lenses.streamreactor.connect.http.sink.client.HttpRequestSender
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import io.lenses.streamreactor.connect.http.sink.tpl.TemplateType
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AsyncFunSuiteLike
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.Queue

class HttpWriterTest extends AsyncIOSpec with AsyncFunSuiteLike with Matchers with MockitoSugar {

  private val sinkName = "MySinkName"

  private val topicPartition: TopicPartition    = Topic("myTopic").withPartition(1)
  private val defaultContext: HttpCommitContext = HttpCommitContext.default("My Sink")

  test("add method should add records to the queue") {
    val commitPolicy = CommitPolicy(Count(2L))
    val senderMock   = mock[HttpRequestSender]
    val templateMock = mock[TemplateType]

    for {
      recordsQueueRef  <- Ref.of[IO, Queue[RenderedRecord]](Queue.empty)
      commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default("My Sink"))
      httpWriter = new HttpWriter(sinkName,
                                  commitPolicy,
                                  senderMock,
                                  templateMock,
                                  recordsQueueRef,
                                  commitContextRef,
                                  5,
      )
      recordsToAdd = Seq(
        RenderedRecord(topicPartition.atOffset(100), "record1", Seq.empty, None),
        RenderedRecord(topicPartition.atOffset(101), "record2", Seq.empty, None),
      )
      _ <- httpWriter.add(recordsToAdd)

      queue <- recordsQueueRef.get
    } yield {
      queue shouldBe Queue(recordsToAdd: _*)
    }
  }

  test("process method should flush records when the queue is non-empty and commit policy requires flush") {
    val commitPolicy = CommitPolicy(Count(2L))
    val senderMock   = mock[HttpRequestSender]
    when(senderMock.sendHttpRequest(any[ProcessedTemplate])).thenReturn(IO.unit)

    val templateMock = mock[TemplateType]
    when(templateMock.process(any[Seq[RenderedRecord]])).thenReturn(Right(ProcessedTemplate("a", "b", Seq.empty)))

    val recordsToAdd = Seq(
      RenderedRecord(topicPartition.atOffset(100), "record1", Seq.empty, None),
      RenderedRecord(topicPartition.atOffset(101), "record2", Seq.empty, None),
    )

    {
      for {
        recordsQueueRef  <- Ref.of[IO, Queue[RenderedRecord]](Queue.empty)
        commitContextRef <- Ref.of[IO, HttpCommitContext](defaultContext)

        httpWriter = new HttpWriter(sinkName,
                                    commitPolicy,
                                    senderMock,
                                    templateMock,
                                    recordsQueueRef,
                                    commitContextRef,
                                    5,
        )

        _              <- httpWriter.add(recordsToAdd)
        _              <- httpWriter.process()
        updatedContext <- commitContextRef.get
        updatedQueue   <- recordsQueueRef.get
      } yield {
        updatedContext should not be defaultContext
        updatedQueue shouldBe empty
      }
    }
  }

  test("process method should not flush records when the queue is empty") {
    val commitPolicy = CommitPolicy(Count(2L))
    val senderMock   = mock[HttpRequestSender]
    when(senderMock.sendHttpRequest(any[ProcessedTemplate])).thenReturn(IO.unit)

    val templateMock = mock[TemplateType]

    for {
      recordsQueueRef  <- Ref.of[IO, Queue[RenderedRecord]](Queue.empty)
      commitContextRef <- Ref.of[IO, HttpCommitContext](defaultContext)

      httpWriter = new HttpWriter(sinkName,
                                  commitPolicy,
                                  senderMock,
                                  templateMock,
                                  recordsQueueRef,
                                  commitContextRef,
                                  5,
      )

      _              <- httpWriter.process()
      updatedContext <- commitContextRef.get
      updatedQueue   <- recordsQueueRef.get
    } yield {
      updatedContext shouldBe defaultContext
      updatedQueue shouldBe empty
    }
  }
}
