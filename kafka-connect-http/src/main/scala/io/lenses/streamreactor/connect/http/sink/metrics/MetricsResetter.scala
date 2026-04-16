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
package io.lenses.streamreactor.connect.http.sink.metrics
import cats.effect.Clock
import cats.effect.IO
import cats.effect.Resource
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.http.sink.metrics.MetricsResetter.calculateInitialDelay

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.SECONDS

object MetricsResetter {
  def calculateInitialDelay(interval: FiniteDuration)(implicit clock: Clock[IO]): IO[FiniteDuration] =
    clock.realTimeInstant.flatMap { nowInstant =>
      val intervalSeconds     = interval.toSeconds
      val nowEpochSeconds     = nowInstant.getEpochSecond
      val remainder           = nowEpochSeconds % intervalSeconds
      val initialDelaySeconds = if (remainder == 0) 0 else intervalSeconds - remainder
      IO.pure(FiniteDuration(initialDelaySeconds, SECONDS))
    }
}
class MetricsResetter(
  metrics:        HttpSinkMetricsMBean,
  resetInterval:  FiniteDuration,
  updateInterval: FiniteDuration,
)(
  implicit
  clock: Clock[IO],
) extends StrictLogging {

  // Define the reset task
  private def runReset: IO[Unit] =
    IO(metrics.resetRequestTime())
      .handleErrorWith { error =>
        IO(logger.error("Error resetting metrics", error))
      }

  // Define the update percentiles task
  private def runUpdatePercentiles: IO[Unit] =
    IO(metrics.updatePercentiles())
      .handleErrorWith { error =>
        IO(logger.error("Error updating percentiles", error))
      }

  // Define the scheduling loop for reset
  private def scheduleReset: IO[Unit] = {
    def loop: IO[Unit] = for {
      _ <- IO.sleep(resetInterval)
      _ <- runReset
      _ <- loop
    } yield ()

    calculateInitialDelay(resetInterval).flatMap { initialDelay =>
      IO.sleep(initialDelay) *> runReset *> loop
    }.handleErrorWith { error =>
      IO(logger.error("Error in metrics reset task", error)) *> loop
    }
  }

  // Define the scheduling loop for updating percentiles
  private def scheduleUpdatePercentiles: IO[Unit] = {
    def loop: IO[Unit] = for {
      _ <- IO.sleep(updateInterval)
      _ <- runUpdatePercentiles
      _ <- loop
    } yield ()

    IO.sleep(updateInterval) *> runUpdatePercentiles *> loop
      .handleErrorWith { error =>
        IO(logger.error("Error in metrics update percentiles task", error)) *> loop
      }
  }

  // Resources to manage both scheduled tasks
  def scheduleResetAndUpdate: Resource[IO, Unit] =
    for {
      _ <- Resource.make(scheduleReset.start)(_.cancel)
      _ <- Resource.make(scheduleUpdatePercentiles.start)(_.cancel)
    } yield ()
}
