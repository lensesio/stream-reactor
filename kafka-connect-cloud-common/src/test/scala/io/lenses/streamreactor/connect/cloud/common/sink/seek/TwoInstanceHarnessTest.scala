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

import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.Executors
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

class TwoInstanceHarnessTest extends AnyFunSuite with Matchers with EitherValues {

  private val tp = Topic("topic").withPartition(0)
  private val pk = "date=2024-01-01"

  test("instances share lock paths but have distinct lock owners") {
    val h = new TwoInstanceHarness()
    try {
      h.lockUuid(0) should not be h.lockUuid(1)
      h.taskIds(0).name shouldBe h.taskIds(1).name
    } finally h.close()
  }

  test("a granular commit by instance A is visible to instance B after B opens the partition") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(5)), None).value
      h.a.updateMasterLock(tp, Offset(3)).value

      h.b.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(2)))

      val master = h.readLock(h.masterLockPath(tp)).value.wrappedObject
      master.owner shouldBe h.lockUuid(1)

      h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(5))
      h.readLock(h.granularLockPath(tp, pk)).value.wrappedObject.owner shouldBe h.lockUuid(0)
    } finally h.close()
  }

  test("Gate pauses one thread until released") {
    val gate = new TwoInstanceHarness.Gate
    val pool = Executors.newSingleThreadExecutor()
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
    try {
      val paused = Future { gate.pause(); "resumed" }
      gate.awaitReached()
      paused.isCompleted shouldBe false
      gate.release()
      Await.result(paused, 5.seconds) shouldBe "resumed"
    } finally {
      val _ = pool.shutdownNow()
    }
  }
}
