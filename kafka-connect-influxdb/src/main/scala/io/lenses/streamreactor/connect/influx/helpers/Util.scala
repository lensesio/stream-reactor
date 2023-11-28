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
package io.lenses.streamreactor.connect.influx.helpers

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Util {

  def shortCircuitOnFailure[T](a: Try[Seq[T]], b: Try[T]): Try[Seq[T]] = (a, b) match {
    case (Failure(e), _)              => Failure(e)
    case (Success(r), Success(value)) => Success(r :+ value)
    case (_, Failure(e))              => Failure(e)
  }

  def caseInsensitiveComparison(a: String, b: String): Boolean = a.toUpperCase == b.toUpperCase

  def caseInsensitiveComparison(a: Seq[String], b: Seq[String]): Boolean = a.map(_.toUpperCase) == b.map(_.toUpperCase)

  val KEY_CONSTANT     = "_key"
  val KEY_All_ELEMENTS = Vector(KEY_CONSTANT, "*")

}
