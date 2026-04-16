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
package io.lenses.streamreactor.metrics

import cats.effect.Clock
import cats.effect.Sync
import cats.syntax.all._

import scala.concurrent.duration._

object Metrics {

  def withTimerF[F[_], A](
    block: => F[A],
  )(logF:  Long => F[Unit],
  )(
    implicit
    F:     Sync[F],
    clock: Clock[F],
  ): F[A] =
    for {
      start       <- clock.monotonic
      result      <- block.attempt
      end         <- clock.monotonic
      duration     = Duration(end.toMillis - start.toMillis, MILLISECONDS)
      _           <- logF(duration.toMillis)
      finalResult <- result.liftTo[F]
    } yield finalResult

  def withTimer[A](block: => A)(logF: Long => Unit): A = {
    val start = System.nanoTime()
    try {
      block
    } finally {
      val end      = System.nanoTime()
      val duration = end - start
      logF(duration / 1000000)
    }
  }
}
