/*
 * Copyright 2017-2024 Lenses.io Ltd
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
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CyclopsToScalaEitherTest extends AnyFunSuite with Matchers with EitherValues {

  test("CyclopsToScalaEither should convert a Cyclops Either with Right value to Scala Either") {
    val cyclopsEither: CyclopsEither[String, Int] = CyclopsEither.right(10)
    val scalaEither = CyclopsToScalaEither.convertToScalaEither(cyclopsEither)

    scalaEither.value should be(10)
  }

  test("CyclopsToScalaEither should convert a Cyclops Either with Left value to Scala Either") {
    val cyclopsEither: CyclopsEither[String, Int] = CyclopsEither.left("Error")
    val scalaEither = CyclopsToScalaEither.convertToScalaEither(cyclopsEither)

    scalaEither.left.value should be("Error")
  }

}
