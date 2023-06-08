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

import cats.implicits.catsSyntaxEitherId
import cats.implicits.toBifunctorOps

import scala.util.Try

object IteratorOps {
  def skip[T](
    iterator: Iterator[T],
    skip:     Int,
  ): Either[NoSuchElementException, Unit] = {
    def nextOrError(index: Int): Either[NoSuchElementException, Unit] =
      if (iterator.hasNext) {
        Try(iterator.next()).map(_ => ()).toEither.leftMap(_ =>
          new NoSuchElementException("Failed to skip to line " + index),
        )
      } else {
        new NoSuchElementException(s"No more items at index $index").asLeft[Unit]
      }

    (0 until skip).foldLeft(().asRight[NoSuchElementException])((acc, i) => acc.flatMap(_ => nextOrError(i)))
  }
}
