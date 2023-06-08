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
package io.lenses.streamreactor.connect.io.text

import cats.implicits.{catsSyntaxOptionId, none}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ListBuffer

class OptionIteratorAdaptorTest extends AnyFunSuite with Matchers {

  test("should iterate over a sequence of Option[String]") {

    val optStringFncs = ListBuffer("1".some, "2".some, "3".some, none)
    val strFunc       = () => optStringFncs.remove(0)

    val adaptor = new OptionIteratorAdaptor(strFunc)

    adaptor.hasNext should be(true)
    adaptor.next() should be("1")

    adaptor.hasNext should be(true)
    adaptor.next() should be("2")

    adaptor.hasNext should be(true)
    adaptor.next() should be("3")

    adaptor.hasNext should be(false)
    assertThrows[IllegalStateException] {
      adaptor.next()
    }
  }

}
