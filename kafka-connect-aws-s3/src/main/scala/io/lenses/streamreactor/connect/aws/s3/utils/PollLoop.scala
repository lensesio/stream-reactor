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
package io.lenses.streamreactor.connect.aws.s3.utils

import cats.effect.IO
import cats.effect.Ref

import scala.concurrent.duration.FiniteDuration

object PollLoop {

  //Calls fn every interval until cancelledRef is set to true
  def run(interval: FiniteDuration, cancelledRef: Ref[IO, Boolean])(fn: () => IO[Unit]): IO[Unit] =
    for {
      _ <- fn()
      _ <- IO.sleep(interval)
      _ <- cancelledRef.get.flatMap { cancelled =>
        if (cancelled) {
          IO.unit
        } else {
          run(interval, cancelledRef)(fn)
        }
      }
    } yield ()
}
