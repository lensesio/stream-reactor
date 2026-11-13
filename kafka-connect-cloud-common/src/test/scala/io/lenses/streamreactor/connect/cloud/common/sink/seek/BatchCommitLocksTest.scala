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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

import cats.data.Validated
import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicBoolean

/** The per-partition lock map shared by `commitBatch` and the batch temp sweep (LC-349). */
class BatchCommitLocksTest extends AnyFunSuiteLike with Matchers with EitherValues {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("locks-test", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => Validated.valid(location)

  private val tp0 = Topic("orders").withPartition(0)
  private val tp1 = Topic("orders").withPartition(1)

  private def bucketAndPrefix(topicPartition: TopicPartition): Either[SinkError, CloudLocation] =
    CloudLocation("test-bucket", Some(s"data/${topicPartition.topic.value}/${topicPartition.partition}/")).asRight

  test("the same lock instance is returned for a partition across revoke and reassign") {
    implicit val storage: InMemoryStorageInterface = new InMemoryStorageInterface()
    val locks = BatchCommitLocks()
    val im = new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      pendingOperationsProcessors = new PendingOperationsProcessors(storage),
      directoryFileName           = ".indexes",
      gcIntervalSeconds           = Int.MaxValue,
      gcSweepIntervalSeconds      = 3600,
      gcSweepMinAgeSeconds        = 3600,
      gcSweepEnabled              = false,
      commitMode                  = CommitMode.Batch,
      batchCommitLocks            = locks,
    )(storage, connectorTaskId)
    try {
      im.open(Set(tp0)).value
      val first = locks.lockFor(tp0)
      // Revoke tp0 (the open of a disjoint set clears its state), then reassign it.
      im.open(Set(tp1)).value
      im.open(Set(tp0)).value
      locks.lockFor(tp0) should be theSameInstanceAs first
    } finally im.close()
  }

  test("entries are never removed") {
    val locks = BatchCommitLocks()
    val a     = locks.lockFor(tp0)
    locks.withLock(tp0)(())
    locks.withLockInterruptibly(tp1)(())
    locks.size shouldBe 2
    locks.lockFor(tp0) should be theSameInstanceAs a
    locks.size shouldBe 2
  }

  test("the lock is fair") {
    BatchCommitLocks().lockFor(tp0).isFair shouldBe true
  }

  test("withLock holds the partition's lock for the body and releases it, also on a throw") {
    val locks = BatchCommitLocks()
    val lock  = locks.lockFor(tp0)
    locks.withLock(tp0)(lock.isHeldByCurrentThread) shouldBe true
    lock.isLocked shouldBe false
    an[IllegalStateException] should be thrownBy locks.withLock(tp0)(throw new IllegalStateException("boom"))
    lock.isLocked shouldBe false
    locks.withLockInterruptibly(tp0)(lock.isHeldByCurrentThread) shouldBe true
    lock.isLocked shouldBe false
  }

  test("withLockInterruptibly on an interrupted thread throws without running the body") {
    val locks = BatchCommitLocks()
    val ran   = new AtomicBoolean(false)
    Thread.currentThread().interrupt()
    try {
      an[InterruptedException] should be thrownBy locks.withLockInterruptibly(tp0)(ran.set(true))
    } finally {
      val _ = Thread.interrupted()
    }
    ran.get() shouldBe false
    locks.lockFor(tp0).isLocked shouldBe false
  }

  test("the no-op implementation runs the body") {
    val ran = new AtomicBoolean(false)
    BatchCommitLocks.NoOp.withLock(tp0) { ran.set(true); 42 } shouldBe 42
    ran.get() shouldBe true
    BatchCommitLocks.NoOp.withLockInterruptibly(tp0)("x") shouldBe "x"
  }
}
