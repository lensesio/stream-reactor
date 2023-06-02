package io.lenses.streamreactor.connect.aws.s3.utils

import cats.effect.IO
import cats.effect.Ref

import scala.concurrent.duration.FiniteDuration

object PollLoop {
  def run(interval: FiniteDuration, cancelledRef: Ref[IO, Boolean])(fn: () => IO[Unit]): IO[Unit] =
    for {
      _         <- IO.sleep(interval)
      cancelled <- cancelledRef.get
      _         <- if (cancelled) IO.unit else fn()
    } yield ()
}
