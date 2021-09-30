package io.lenses.streamreactor.connect.aws.s3.formats

import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.sink.FatalS3SinkError

import scala.util.{Failure, Success, Try}

object Suppress {

  def apply(r: => Unit): Either[FatalS3SinkError, Unit] = {
    Try(r) match {
      case Failure(_) =>
      case Success(_) =>
    }
    ().asRight[FatalS3SinkError]
  }

}
