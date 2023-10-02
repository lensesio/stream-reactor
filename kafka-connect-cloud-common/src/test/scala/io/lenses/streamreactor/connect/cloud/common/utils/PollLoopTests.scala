/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.utils

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global
import io.lenses.streamreactor.connect.cloud.common.utils.PollLoop
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt
class PollLoopTests extends AnyFlatSpecLike with Matchers {
  "PollLoop" should "invoke the call 3 times and then stop" in {
    val count        = new AtomicReference[Int](0)
    val interval     = 100.millis
    val cancelledRef = Ref.unsafe[IO, Boolean](false)
    val fn = () =>
      IO {
        val t = count.accumulateAndGet(1, (a, b) => a + b)
        if (t == 3) {
          cancelledRef.set(true).unsafeRunSync()
        }
      }
    val pollLoop = PollLoop.run(interval, cancelledRef)(fn)
    val fibre    = pollLoop.start.unsafeRunSync()
    fibre.join.unsafeRunSync()
    count.get() should be(3)
  }

  //test with 0 interval
  "PollLoop" should "invoke the call 3 times and then stop with 0 interval" in {
    val count        = new AtomicReference[Int](0)
    val interval     = 0.millis
    val cancelledRef = Ref.unsafe[IO, Boolean](false)
    val fn = () =>
      IO {
        val t = count.accumulateAndGet(1, (a, b) => a + b)
        if (t == 3) {
          cancelledRef.set(true).unsafeRunSync()
        }
      }
    val pollLoop = PollLoop.run(interval, cancelledRef)(fn)
    val fibre    = pollLoop.start.unsafeRunSync()
    fibre.join.unsafeRunSync()
    count.get() should be(3)
  }

  "PollLoop" should "run the task once if it does not fail" in {
    val count        = new AtomicReference[Int](0)
    val interval     = 100.millis
    val cancelledRef = Ref.unsafe[IO, Boolean](false)
    val fn = () =>
      IO {
        val t = count.accumulateAndGet(1, (a, b) => a + b)
        if (t == 3) {
          cancelledRef.set(true).unsafeRunSync()
        }
      }
    val pollLoop = PollLoop.oneOfIgnoreError(interval, cancelledRef, _ => ())(fn)
    val fibre    = pollLoop.start.unsafeRunSync()
    fibre.join.unsafeRunSync()
    count.get() should be(1)
  }
  "PollLoop" should "handle 3 failure and one success of oneOfIgnoreError" in {
    val count        = new AtomicReference[Int](0)
    val interval     = 100.millis
    val cancelledRef = Ref.unsafe[IO, Boolean](false)
    val fn = () =>
      IO {
        val t = count.accumulateAndGet(1, (a, b) => a + b)
        if (t <= 3) {
          throw new RuntimeException("error")
        } else ()
      }
    val pollLoop = PollLoop.oneOfIgnoreError(interval, cancelledRef, _ => ())(fn)
    val fibre    = pollLoop.start.unsafeRunSync()
    fibre.join.unsafeRunSync()
    count.get() should be(4)
  }
  "PollLoop" should "terminate when it is cancelled, if the fn keeps failing" in {
    val count        = new AtomicReference[Int](0)
    val interval     = 100.millis
    val cancelledRef = Ref.unsafe[IO, Boolean](false)
    val fn = () =>
      IO {
        count.accumulateAndGet(1, (a, b) => a + b)
        throw new RuntimeException("error")
      }
    val pollLoop = PollLoop.run(interval, cancelledRef)(fn)
    val fibre    = pollLoop.start.unsafeRunSync()
    Thread.sleep(1000)
    cancelledRef.set(true).unsafeRunSync()
    fibre.join.unsafeRunSync()
    count.get() should be >= 1
  }
}
