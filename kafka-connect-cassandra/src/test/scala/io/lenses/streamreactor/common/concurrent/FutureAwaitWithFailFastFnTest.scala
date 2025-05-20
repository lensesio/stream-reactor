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
package io.lenses.streamreactor.common.concurrent

import io.lenses.streamreactor.common.concurrent.ExecutorExtension._
import org.apache.kafka.connect.errors.ConnectException
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.TimeLimits
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.Executors
import scala.util.Failure
import scala.util.Try

/**
  * Created by stepi on 22/06/16.
  */
class FutureAwaitWithFailFastFnTest extends AnyWordSpec with Matchers with Eventually with TimeLimits {

  "FutureAwaitWithFailFastFn" should {
    "return when all the futures have completed" in {
      val exec = Executors.newFixedThreadPool(10)
      val futures = (1 to 5).map(i =>
        exec.submit {
          Thread.sleep(300)
          i
        },
      )
      eventually {
        val result = FutureAwaitWithFailFastFn(exec, futures)
        exec.isTerminated shouldBe true
        result shouldBe Seq(1, 2, 3, 4, 5)
      }
    }

    "stop when the first futures times out" in {
      val exec = Executors.newFixedThreadPool(6)
      val futures = for (i <- 1 to 10) yield {
        exec.submit {
          if (i == 4) {
            Thread.sleep(1000)
            throw new ConnectException("this task failed.")
          } else {
            Thread.sleep(50000)
          }
        }
      }

      eventually {
        val t = Try(FutureAwaitWithFailFastFn(exec, futures))
        t.isFailure shouldBe true
        t.asInstanceOf[Failure[_]].exception.getMessage shouldBe "this task failed."
        exec.isTerminated shouldBe true
      }
    }
  }

}
