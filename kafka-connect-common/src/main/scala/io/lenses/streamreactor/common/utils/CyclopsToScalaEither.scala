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
package io.lenses.streamreactor.common.utils

import cyclops.control.{ Either => CyclopsEither }

import scala.util.{ Either => ScalaEither }

/**
  * Utility object for converting Cyclops Either to Scala Either.
  *
  * This object provides a method to convert an instance of Cyclops Either to a Scala Either.
  * If the Cyclops Either does not contain a valid value, an InvalidEitherException is thrown.
  */
object CyclopsToScalaEither {

  /**
    * Exception thrown when the Cyclops Either is invalid.
    *
    * This exception is used to indicate that an invalid state was encountered
    * during the conversion of Cyclops Either to Scala Either.  It is not
    * expected that this exception will ever surface, assuming that the
    * implementation of Cyclops Either is correct.  We can only throw a
    * RuntimeException here as it is not possible to produce a L for this
    * Either as we do not know the type, and providing a function that creates
    * the type whenever you convert an Either would be inconvenient for the
    * caller and overkill for a scenario that should never occur.
    */
  class InvalidEitherException() extends RuntimeException

  /**
    * Converts a Cyclops Either to a Scala Either.
    *
    * This method converts an instance of Cyclops Either to a Scala Either. If the Cyclops Either
    * is a Right value, it is converted to a Scala Right. If the Cyclops Either is a Left value,
    * it is converted to a Scala Left. If the Cyclops Either is invalid, an InvalidEitherException
    * is thrown.  This is not expected to ever happen.
    *
    * @tparam L the type of the Left value
    * @tparam R the type of the Right value
    * @param cyclopsEither the Cyclops Either to convert
    * @return the converted Scala Either
    * @throws InvalidEitherException if the Cyclops Either is invalid
    */
  def convertToScalaEither[L, R](cyclopsEither: CyclopsEither[L, R]): ScalaEither[L, R] =
    Either.cond(
      cyclopsEither.isRight,
      cyclopsEither.orElseGet(() => throw new InvalidEitherException()),
      cyclopsEither.swap().orElseGet(() => throw new InvalidEitherException()),
    )

}
