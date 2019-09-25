package com.datamountaineer.streamreactor.connect.influx.helpers

import scala.util.{Failure, Success, Try}

object Util {

  def shortCircuitOnFailure[T](a: Try[Seq[T]], b: Try[T]): Try[Seq[T]] = (a, b) match {
    case (Failure(e), _) => Failure(e)
    case (Success(r), Success(value)) => Success(r :+ value)
    case (_, Failure(e)) => Failure(e)
  }

  val KEY_CONSTANT = "_key"
}