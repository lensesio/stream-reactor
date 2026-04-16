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
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.common.batch.BatchPolicy
import io.lenses.streamreactor.common.batch.NonEmptyBatchInfo
import io.lenses.streamreactor.common.batch.EmptyBatchInfo
import io.lenses.streamreactor.common.batch.BatchResult
import io.lenses.streamreactor.common.batch.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import org.apache.kafka.connect.errors.RetriableException
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AsyncFunSuiteLike
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.Queue
import scala.concurrent.duration.DurationInt

class RecordsQueueTest extends AsyncFunSuiteLike with AsyncIOSpec with MockitoSugar with Matchers {

  private val timestamp = 125L

  private val defaultContext: HttpCommitContext = HttpCommitContext.default("My Sink")

  private val topicPartition: TopicPartition = Topic("myTopic").withPartition(1)

  private val testEndpoint = "https://mytestendpoint.example.com"

  private val record1 = RenderedRecord(topicPartition.atOffset(100), timestamp, "record1", Seq.empty, testEndpoint)
  private val record2 = RenderedRecord(topicPartition.atOffset(101), timestamp, "record2", Seq.empty, testEndpoint)
  private val record3 = RenderedRecord(topicPartition.atOffset(102), timestamp, "record3", Seq.empty, testEndpoint)

  test("enqueueAll should add all records to the queue") {
    {
      for {
        commitContext   <- Ref[IO].of(defaultContext)
        recordsQueueRef <- Ref[IO].of(Queue.empty[RenderedRecord])
        offsetsRef      <- Ref[IO].of(Map.empty[TopicPartition, Offset])
        recordsQueue     = new RecordsQueue(recordsQueueRef, commitContext, mock[BatchPolicy], 10000, 1.minute, offsetsRef)
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
    val commitPolicy = mock[BatchPolicy]
    when(commitPolicy.shouldBatch(any[HttpCommitContext])).thenReturn(BatchResult(fitsInBatch          = false,
                                                                                  triggerReached       = false,
                                                                                  greedyTriggerReached = false,
    ))

    {
      for {
        commitContext   <- Ref[IO].of(defaultContext)
        recordsQueueRef <- Ref[IO].of(Queue(record1, record2))
        offsetsRef      <- Ref[IO].of(Map.empty[TopicPartition, Offset])
        recordsQueue     = new RecordsQueue(recordsQueueRef, commitContext, commitPolicy, 10000, 1.minute, offsetsRef)
        batchInfo       <- recordsQueue.popBatch()
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
        offsetsRef      <- Ref[IO].of(Map.empty[TopicPartition, Offset])
        recordsQueue     = new RecordsQueue(recordsQueueRef, commitContext, mock[BatchPolicy], 10000, 1.minute, offsetsRef)
        batchInfo       <- recordsQueue.popBatch()
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
        offsetsRef      <- Ref[IO].of(Map.empty[TopicPartition, Offset])
        recordsQueue     = new RecordsQueue(recordsQueueRef, commitContext, mock[BatchPolicy], 10000, 1.minute, offsetsRef)
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
        offsetsRef      <- Ref[IO].of(Map.empty[TopicPartition, Offset])
        recordsQueue     = new RecordsQueue(recordsQueueRef, commitContext, mock[BatchPolicy], 10000, 1.minute, offsetsRef)
        _               <- recordsQueue.dequeue(records)
        refValue        <- recordsQueueRef.get
      } yield refValue
    } asserting {
      backingQueue =>
        backingQueue should contain theSameElementsInOrderAs Seq(record1)
    }
  }

  test("enqueue all should throw RetriableException if the queue size is exceeded") {
    val records      = NonEmptySeq.of(record1, record2)
    val commitPolicy = mock[BatchPolicy]

    val ioAction = for {
      commitContext   <- Ref[IO].of(defaultContext)
      recordsQueueRef <- Ref[IO].of(Queue.empty[RenderedRecord])
      offsetsRef      <- Ref[IO].of(Map.empty[TopicPartition, Offset])
      recordsQueue     = new RecordsQueue(recordsQueueRef, commitContext, commitPolicy, 1, 2.seconds, offsetsRef)
      _               <- recordsQueue.enqueueAll(records)
    } yield ()

    ioAction.attempt.map {
      case Left(e: RetriableException) =>
        e.getMessage should be("Enqueue timed out and records remain")
      case Left(e) =>
        fail(s"Expected RetriableException but got ${e}")
      case Right(_) =>
        fail("Expected RetriableException but enqueueAll succeeded")
    }

  }

  test(" does not enqueue a record which was enqueued before") {
    val records      = NonEmptySeq.of(record1, record2)
    val commitPolicy = mock[BatchPolicy]
    val newRecords   = NonEmptySeq.of(record2, record3)

    val ioAction = for {
      commitContext   <- Ref[IO].of(defaultContext)
      recordsQueueRef <- Ref[IO].of(Queue.empty[RenderedRecord])
      offsetsRef      <- Ref[IO].of(Map.empty[TopicPartition, Offset])
      recordsQueue     = new RecordsQueue(recordsQueueRef, commitContext, commitPolicy, 10000, 1.minute, offsetsRef)
      _               <- recordsQueue.enqueueAll(records)
      _               <- recordsQueue.enqueueAll(newRecords)
      queue           <- recordsQueueRef.get
    } yield queue.toList

    ioAction asserting {
      queue =>
        queue should contain theSameElementsInOrderAs List(record1, record2, record3)
    }
  }

  test("enqueue when the queue size is 1") {
    val records      = NonEmptySeq.of(record1)
    val commitPolicy = mock[BatchPolicy]
    val newRecords   = NonEmptySeq.of(record2)

    val ioAction = for {
      commitContext   <- Ref[IO].of(defaultContext)
      recordsQueueRef <- Ref[IO].of(Queue.empty[RenderedRecord])
      offsetsRef      <- Ref[IO].of(Map.empty[TopicPartition, Offset])
      recordsQueue     = new RecordsQueue(recordsQueueRef, commitContext, commitPolicy, 1, 10.seconds, offsetsRef)
      _               <- recordsQueue.enqueueAll(records)
      _               <- recordsQueue.dequeue(records)
      _               <- recordsQueue.enqueueAll(newRecords)
      queue           <- recordsQueueRef.get
    } yield queue.toList

    ioAction asserting {
      queue =>
        queue should contain theSameElementsInOrderAs List(record2)
    }
  }
}
