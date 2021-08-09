/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink

import com.typesafe.scalalogging.LazyLogging

/**
  * For java compatbility, transforms our nice Either[Throwable, A] to a thrown exception.
  *
  * @param e either of a Throwable or type A
  * @tparam A custom type
  */
case class ThrowableEither[A](e: Either[Throwable, A]) extends LazyLogging {
  def toThrowable(sinkName: String): A = e match {
    case Left(ex) => {
      logger.error(s"[$sinkName] Error", ex)
      throw ex
    }
    case Right(a: A) => a
  }
}

object ThrowableEither {

  implicit def toJavaThrowableConverter[A](e: Either[Throwable, A]): ThrowableEither[A] = ThrowableEither(e)

}
