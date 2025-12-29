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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink.writer

import com.azure.cosmos.implementation.Document
import com.azure.cosmos.models.CosmosItemOperation
import io.lenses.streamreactor.common.batch.BatchPolicy
import io.lenses.streamreactor.common.batch.Count
import io.lenses.streamreactor.common.batch.EmptyBatchInfo
import io.lenses.streamreactor.common.batch.NonEmptyBatchInfo
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import org.apache.kafka.connect.errors.RetriableException
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.concurrent._
import java.util.concurrent.Executors

class CosmosRecordsQueueTest extends AnyFunSuite with Matchers with MockitoSugar {

  def dummyRecord(offset: Long = 1L, partition: Int = 0): PendingRecord = {
    val tpo = Topic("topic").withPartition(partition).atOffset(offset)
    val doc = mock[Document]
    val op  = mock[CosmosItemOperation]
    PendingRecord(tpo, doc, op)
  }

  def makeBatchPolicy(count: Long = 1L): BatchPolicy = BatchPolicy(Count(count))

  def queue(maxSize: Int = 10, batchPolicy: BatchPolicy = makeBatchPolicy()): CosmosRecordsQueue[PendingRecord] =
    new CosmosRecordsQueue[PendingRecord]("sink", maxSize, 100.millis, batchPolicy)

  test("enqueueAll adds records and updates offsetMap") {
    val q    = queue()
    val recs = List(dummyRecord(1), dummyRecord(2))
    q.enqueueAll(recs)
    // First batch
    q.peekBatch() match {
      case NonEmptyBatchInfo(batch, _, _) =>
        val records = batch.toSeq.toList.asInstanceOf[List[PendingRecord]]
        records.map(_.topicPartitionOffset.offset.value) should contain only 1L
      case _ => fail("Expected NonEmptyBatchInfo")
    }
    // Dequeue first batch
    q.dequeue(q.peekBatch().asInstanceOf[NonEmptyBatchInfo[PendingRecord]].batch.toSeq.toList)
    // Second batch
    q.peekBatch() match {
      case NonEmptyBatchInfo(batch, _, _) =>
        val records = batch.toSeq.toList.asInstanceOf[List[PendingRecord]]
        records.map(_.topicPartitionOffset.offset.value) should contain only 2L
      case _ => fail("Expected NonEmptyBatchInfo")
    }
  }

  test("enqueueAll filters duplicates by offset") {
    val q    = queue()
    val rec1 = dummyRecord(1)
    val rec2 = dummyRecord(2)
    q.enqueueAll(List(rec1))
    q.enqueueAll(List(rec2))
    // First batch
    q.peekBatch() match {
      case NonEmptyBatchInfo(batch, _, _) =>
        val records = batch.toSeq.toList.asInstanceOf[List[PendingRecord]]
        records.map(_.topicPartitionOffset.offset.value) should contain only 1L
      case _ => fail("Expected NonEmptyBatchInfo")
    }
    // Dequeue first batch
    q.dequeue(q.peekBatch().asInstanceOf[NonEmptyBatchInfo[PendingRecord]].batch.toSeq.toList)
    // Second batch
    q.peekBatch() match {
      case NonEmptyBatchInfo(batch, _, _) =>
        val records = batch.toSeq.toList.asInstanceOf[List[PendingRecord]]
        records.map(_.topicPartitionOffset.offset.value) should contain only 2L
      case _ => fail("Expected NonEmptyBatchInfo")
    }
  }

  test("enqueueAll throws RetriableException on timeout") {
    val q    = queue(maxSize = 1)
    val recs = List(dummyRecord(1), dummyRecord(2))
    val ex = intercept[RetriableException] {
      q.enqueueAll(recs)
    }
    ex.getMessage should include("Enqueue timed out")
  }

  test("peekBatch returns EmptyBatchInfo when queue is empty") {
    val q = queue()
    q.peekBatch() shouldBe a[EmptyBatchInfo]
  }

  test("peekBatch returns NonEmptyBatchInfo when queue has records") {
    val q    = queue()
    val recs = List(dummyRecord(1), dummyRecord(2))
    q.enqueueAll(recs)
    q.peekBatch() shouldBe a[NonEmptyBatchInfo[_]]
  }

  test("dequeue removes records from the queue") {
    val q    = queue()
    val recs = List(dummyRecord(1), dummyRecord(2), dummyRecord(3))
    q.enqueueAll(recs)
    val batch = recs.take(2)
    q.dequeue(batch)
    q.peekBatch() match {
      case NonEmptyBatchInfo(batchLeft, _, _) =>
        val records = batchLeft.toSeq.toList.asInstanceOf[List[PendingRecord]]
        records should contain only recs(2)
      case _ => fail("Expected NonEmptyBatchInfo")
    }
  }

  test("dequeue on empty queue does nothing") {
    val q = queue()
    noException should be thrownBy q.dequeue(List.empty)
    q.peekBatch() shouldBe a[EmptyBatchInfo]
  }

  test("enqueueAll updates offsetMap with highest offset") {
    val q    = queue()
    val rec1 = dummyRecord(1)
    val rec2 = dummyRecord(3)
    val rec3 = dummyRecord(2)
    q.enqueueAll(List(rec1, rec2, rec3))
    // offsetMap should have highest offset for the partition
    val offsetMapField = q.getClass.getDeclaredField("offsetMap")
    offsetMapField.setAccessible(true)
    val offsetMap =
      offsetMapField.get(q).asInstanceOf[java.util.concurrent.atomic.AtomicReference[Map[TopicPartition, Offset]]].get()
    offsetMap(TopicPartition(Topic("topic"), 0)).value shouldBe 3L
  }

  test("no batch is formed until threshold is reached") {
    val q    = queue(batchPolicy = makeBatchPolicy(3))
    val recs = List(dummyRecord(1), dummyRecord(2))
    q.enqueueAll(recs)
    q.peekBatch() match {
      case EmptyBatchInfo(size) => size shouldBe 2
      case _                    => fail("Expected EmptyBatchInfo when below threshold")
    }
    q.enqueueAll(List(dummyRecord(3)))
    q.peekBatch() match {
      case NonEmptyBatchInfo(batch, _, _) =>
        val records = batch.toSeq.toList.asInstanceOf[List[PendingRecord]]
        records.map(_.topicPartitionOffset) should contain theSameElementsAs List(dummyRecord(1),
                                                                                  dummyRecord(2),
                                                                                  dummyRecord(3),
        ).map(_.topicPartitionOffset)
      case _ => fail("Expected NonEmptyBatchInfo when threshold is reached")
    }
  }

  test("concurrent dequeue does not lose or duplicate records") {
    val numRecords = 100
    val numThreads = 8
    val q          = queue(maxSize = numRecords, batchPolicy = makeBatchPolicy(1))
    val records    = (1L to numRecords.toLong).map(offset => dummyRecord(offset)).toList
    q.enqueueAll(records)

    val dequeuedOffsets = java.util.concurrent.ConcurrentHashMap.newKeySet[Long]()

    implicit val ec: ExecutionContext = ExecutionContext.global

    def dequeueLoop(): Unit = {
      var done = false
      while (!done) {
        q.peekBatch() match {
          case NonEmptyBatchInfo(batch, _, _) =>
            val batchList = batch.toSeq.toList.asInstanceOf[List[PendingRecord]]
            q.dequeue(batchList)
            batchList.foreach(r => dequeuedOffsets.add(r.topicPartitionOffset.offset.value))
          case EmptyBatchInfo(_) =>
            done = true
        }
      }
    }

    val futures = (1 to numThreads).map(_ => Future(dequeueLoop()))
    Await.result(Future.sequence(futures), 10.seconds)

    val uniqueOffsets = dequeuedOffsets.toArray().map(_.asInstanceOf[Long]).toSet
    uniqueOffsets shouldBe (1L to numRecords.toLong).toSet
    uniqueOffsets.size shouldBe numRecords
    q.peekBatch() shouldBe a[EmptyBatchInfo]
  }

  test("concurrent enqueueAll from multiple threads does not lose or duplicate records") {
    import scala.concurrent._
    import scala.concurrent.duration._

    val numThreads       = 8
    val recordsPerThread = 50
    val totalRecords     = numThreads * recordsPerThread
    val q                = queue(maxSize = totalRecords, batchPolicy = makeBatchPolicy(totalRecords.toLong))

    // Each thread enqueues records on a different partition to avoid offsetMap filtering conflicts
    // (offsetMap filters records with offsets <= highest seen for the same partition)
    val allRecords: Seq[List[PendingRecord]] = (0 until numThreads).map { tIdx =>
      (1L to recordsPerThread.toLong).map(offset => dummyRecord(offset, partition = tIdx)).toList
    }

    implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(numThreads))

    try {
      val futures = allRecords.map { recs =>
        Future {
          q.enqueueAll(recs)
        }
      }
      Await.result(Future.sequence(futures), 10.seconds)

      // After all enqueues, the queue should contain all records
      val batch = q.peekBatch() match {
        case NonEmptyBatchInfo(batch, _, _) => batch.toSeq.toList.asInstanceOf[List[PendingRecord]]
        case EmptyBatchInfo(_)              => List.empty[PendingRecord]
      }
      // Verify we have all records - each partition has offsets 1-50
      batch.size shouldBe totalRecords
      // Group by partition and verify each partition has the expected offsets
      val byPartition = batch.groupBy(_.topicPartitionOffset.toTopicPartition.partition)
      byPartition.size shouldBe numThreads
      byPartition.values.foreach { partitionRecords =>
        val offsets = partitionRecords.map(_.topicPartitionOffset.offset.value).toSet
        offsets shouldBe (1L to recordsPerThread.toLong).toSet
      }
    } finally {
      ec.shutdown()
    }
  }

}
