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

import cats.data.NonEmptySeq
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.testing.scalatest.AsyncIOSpec
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AsyncFunSuiteLike
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.Queue

class RecordsQueueTest extends AsyncFunSuiteLike with AsyncIOSpec with MockitoSugar with Matchers {

  private val timestamp = 125L

  private val defaultContext: HttpCommitContext = HttpCommitContext.default("My Sink")

  private val topicPartition: TopicPartition = Topic("myTopic").withPartition(1)

  private val testEndpoint = "https://mytestendpoint.example.com"

  private val record1 = RenderedRecord(topicPartition.atOffset(100), timestamp, "record1", Seq.empty, testEndpoint)
  private val record2 = RenderedRecord(topicPartition.atOffset(101), timestamp, "record2", Seq.empty, testEndpoint)

  test("enqueueAll should add all records to the queue") {
    {
      for {
        commitContext   <- Ref[IO].of(defaultContext)
        recordsQueueRef <- Ref[IO].of(Queue.empty[RenderedRecord])
        recordsQueue     = new RecordsQueue(recordsQueueRef, commitContext, mock[CommitPolicy])
        _               <- recordsQueue.enqueueAll(NonEmptySeq.of(record1, record2))
        refValue        <- recordsQueueRef.get
      } yield refValue
    } asserting {
      backingQueue =>
        backingQueue.size should be(2)
        backingQueue should contain theSameElementsInOrderAs Seq(record1, record2)
    }
  }

  test("takeBatch should not return a batch of records when commit policy does not require flush") {
    val commitPolicy = mock[CommitPolicy]
    when(commitPolicy.shouldFlush(any[HttpCommitContext])).thenReturn(false)

    {
      for {
        commitContext   <- Ref[IO].of(defaultContext)
        recordsQueueRef <- Ref[IO].of(Queue(record1, record2))
        recordsQueue     = new RecordsQueue(recordsQueueRef, commitContext, commitPolicy)
        batchInfo       <- recordsQueue.takeBatch()
      } yield batchInfo
    } asserting {
      case EmptyBatchInfo(totalQueueSize) => totalQueueSize shouldBe 2
      case NonEmptyBatchInfo(_, _, _)     => fail("Should be an empty batch info")
    }
  }

  test("takeBatch should return an empty batch when the queue is empty") {
    {
      for {
        commitContext   <- Ref[IO].of(defaultContext)
        recordsQueueRef <- Ref[IO].of(Queue.empty[RenderedRecord])
        recordsQueue     = new RecordsQueue(recordsQueueRef, commitContext, mock[CommitPolicy])
        batchInfo       <- recordsQueue.takeBatch()
      } yield batchInfo
    } asserting {
      case EmptyBatchInfo(totalQueueSize) => totalQueueSize shouldBe 0
      case NonEmptyBatchInfo(_, _, _)     => fail("Should be an empty BatchInfo")
    }
  }

  test("dequeue should remove the specified records from the queue") {
    val records = NonEmptySeq.of(record1)

    {
      for {
        commitContext   <- Ref[IO].of(defaultContext)
        recordsQueueRef <- Ref[IO].of(Queue(record1, record2))
        recordsQueue     = new RecordsQueue(recordsQueueRef, commitContext, mock[CommitPolicy])
        _               <- recordsQueue.dequeue(records)
        refValue        <- recordsQueueRef.get
      } yield refValue
    } asserting {
      backingQueue =>
        backingQueue should contain theSameElementsInOrderAs Seq(record2)
    }
  }

  test("dequeue should do nothing if the specified records are not in the queue") {
    val records = NonEmptySeq.of(record2)

    {
      for {
        commitContext   <- Ref[IO].of(defaultContext)
        recordsQueueRef <- Ref[IO].of(Queue(record1))
        recordsQueue     = new RecordsQueue(recordsQueueRef, commitContext, mock[CommitPolicy])
        _               <- recordsQueue.dequeue(records)
        refValue        <- recordsQueueRef.get
      } yield refValue
    } asserting {
      backingQueue =>
        backingQueue should contain theSameElementsInOrderAs Seq(record1)
    }
  }
}
