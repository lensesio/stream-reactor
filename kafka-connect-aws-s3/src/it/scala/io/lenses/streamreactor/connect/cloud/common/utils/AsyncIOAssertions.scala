package io.lenses.streamreactor.connect.cloud.common.utils

import cats.effect.IO

/**
  * This is a workaround for being unable to use AsyncIOSpec in some tests due to their superclasses using some other non-IO spec.
  */
trait AsyncIOAssertions {

  implicit class IOAssertionOps[T](io: IO[T]) {
    def asserting(predicate: T => Unit): IO[T] = io.map { value =>
      predicate(value)
      value
    }
  }

}
