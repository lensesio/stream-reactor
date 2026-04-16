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
package io.lenses.streamreactor.common.utils

object EitherOps {
  implicit class EitherOps[R](val either: Either[Throwable, R]) {
    def unpackOrThrow: R = either match {
      case Right(value) => value
      case Left(ex)     => throw ex
    }

    def unpackOrThrow[T <: Throwable](exceptionMapper: Throwable => T): R = either match {
      case Right(value) => value
      case Left(ex)     => throw exceptionMapper(ex)
    }
  }

}
