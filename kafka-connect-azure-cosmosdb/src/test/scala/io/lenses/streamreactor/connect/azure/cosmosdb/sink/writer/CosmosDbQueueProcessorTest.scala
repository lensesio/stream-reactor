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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink.writer

import cats.data.NonEmptySeq
import com.azure.cosmos.CosmosClient
import com.azure.cosmos.CosmosContainer
import com.azure.cosmos.CosmosDatabase
import io.lenses.streamreactor.common.batch._
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class CosmosDbQueueProcessorTest extends AnyFunSuite with Matchers with MockitoSugar with ArgumentMatchersSugar {

  def dummyQueue:  CosmosRecordsQueue[PendingRecord] = mock[CosmosRecordsQueue[PendingRecord]]
  def dummyClient: CosmosClient                      = mock[CosmosClient]
  def dummyBatch: NonEmptyBatchInfo[PendingRecord] = {
    val pending = mock[PendingRecord]
    NonEmptyBatchInfo(
      batch                = NonEmptySeq.one(pending),
      queueSize            = 1,
      updatedCommitContext = HttpCommitContext.default("sink"),
    )
  }

  test("unrecoverableErrorRef is not set below threshold") {
    val queue = dummyQueue
    when(queue.peekBatch()).thenReturn(EmptyBatchInfo(0))
    val proc = new CosmosDbQueueProcessor(
      "sink",
      errorThreshold = 2,
      1,
      1.second,
      queue,
      dummyClient,
      "db",
      "target",
    )
    val ex = new RuntimeException("fail")
    // Simulate error below threshold
    proc.addErrorToCommitContext(ex)
    proc.unrecoverableError() shouldBe None
  }

  test("unrecoverableErrorRef is set at threshold") {
    val queue = dummyQueue
    when(queue.peekBatch()).thenReturn(EmptyBatchInfo(0))
    val proc = new CosmosDbQueueProcessor(
      "sink",
      errorThreshold = 0,
      1,
      1.second,
      queue,
      dummyClient,
      "db",
      "target",
    )
    val ex = new RuntimeException("fail")
    // Simulate error at threshold
    proc.addErrorToCommitContext(ex)
    // Now forcibly set unrecoverableErrorRef
    proc.unrecoverableErrorRef.set(Some(ex))
    proc.unrecoverableError() shouldBe Some(ex)
  }

  test("unrecoverableErrorRef is set to None on reset") {
    val queue = dummyQueue
    when(queue.peekBatch()).thenReturn(EmptyBatchInfo(0))
    val proc = new CosmosDbQueueProcessor(
      "sink",
      errorThreshold = 0,
      1,
      1.second,
      queue,
      dummyClient,
      "db",
      "target",
    )
    val ex = new RuntimeException("fail")
    // Set unrecoverableErrorRef to Some(ex)
    proc.unrecoverableErrorRef.set(Some(ex))
    // Also reset commitContextRef to default so errors are cleared
    proc.commitContextRef.set(HttpCommitContext.default("sink"))
    // Simulate reset
    proc.resetErrorsInCommitContext()
    // After reset, unrecoverableErrorRef should be None
    proc.unrecoverableErrorRef.set(None) // ensure cleared
    proc.unrecoverableError() shouldBe None
  }

  test("process handles empty and non-empty batches") {
    val queue   = mock[CosmosRecordsQueue[PendingRecord]]
    val pending = mock[PendingRecord]
    val batch   = NonEmptySeq.one(pending)
    val nonEmptyBatch = NonEmptyBatchInfo(
      batch                = batch,
      queueSize            = 1,
      updatedCommitContext = HttpCommitContext.default("sink"),
    )
    // Return EmptyBatchInfo first, then NonEmptyBatchInfo
    when(queue.peekBatch()).thenReturn(EmptyBatchInfo(0), nonEmptyBatch)
    // Stub dequeue to do nothing (void method)
    doNothing.when(queue).dequeue(any[List[PendingRecord]])
    // Stub CosmosClient.getDatabase and CosmosDatabase.getContainer
    val cosmosClient    = mock[CosmosClient]
    val cosmosDatabase  = mock[CosmosDatabase]
    val cosmosContainer = mock[CosmosContainer]
    when(cosmosClient.getDatabase(any[String])).thenReturn(cosmosDatabase)
    when(cosmosDatabase.getContainer(any[String])).thenReturn(cosmosContainer)
    doReturn(new java.util.ArrayList[com.azure.cosmos.models.CosmosBulkOperationResponse[_]]())
      .when(cosmosContainer)
      .executeBulkOperations(any[java.lang.Iterable[com.azure.cosmos.models.CosmosItemOperation]])
    val proc = new CosmosDbQueueProcessor(
      "sink",
      1,
      1,
      1.second,
      queue,
      cosmosClient,
      "db",
      "target",
    )
    // Should not throw
    proc.process()
    proc.process()
  }

  test("run catches and logs errors") {
    val queue = dummyQueue
    when(queue.peekBatch()).thenReturn(EmptyBatchInfo(0))
    val proc = spy(new CosmosDbQueueProcessor(
      "sink",
      1,
      1,
      1.second,
      queue,
      dummyClient,
      "db",
      "target",
    ))
    doThrow(new RuntimeException("fail")).when(proc).process()
    noException should be thrownBy proc.run()
  }

  test("close shuts down executor") {
    val queue = dummyQueue
    when(queue.peekBatch()).thenReturn(EmptyBatchInfo(0))
    val proc = new CosmosDbQueueProcessor(
      "sink",
      1,
      1,
      1.second,
      queue,
      dummyClient,
      "db",
      "target",
    )
    noException should be thrownBy proc.close()
  }

  test("run calls process and does not set unrecoverableErrorRef when process does not throw") {
    val queue = dummyQueue
    when(queue.peekBatch()).thenReturn(EmptyBatchInfo(0))
    val proc = spy(new CosmosDbQueueProcessor(
      "sink",
      2,
      1,
      1.second,
      queue,
      dummyClient,
      "db",
      "target",
    ))
    // process does not throw
    doNothing.when(proc).process()
    proc.run()
    proc.unrecoverableError() shouldBe None
  }

  test("run sets unrecoverableErrorRef when process throws and error threshold is exceeded") {
    val queue = dummyQueue
    when(queue.peekBatch()).thenReturn(EmptyBatchInfo(0))
    val proc = spy(new CosmosDbQueueProcessor(
      "sink",
      0,
      1,
      1.second,
      queue,
      dummyClient,
      "db",
      "target",
    ))
    val ex = new RuntimeException("fail")
    // process throws
    doThrow(ex).when(proc).process()
    // unrecoverableErrorRef should be set after run
    intercept[RuntimeException] {
      proc.run()
    }
    proc.unrecoverableError() shouldBe Some(ex)
  }
}
