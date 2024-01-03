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
package io.lenses.streamreactor.connect.io.text

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.io.InputStream

class SequenceBasedLineReaderTest extends AnyFunSuite with Matchers {
  test("empty input stream returns None") {
    val reader = new SequenceBasedLineReader(createInputStream(""))
    reader.next() should be(None)
  }
  test("no sequence number in the input returns None") {
    val reader = new SequenceBasedLineReader(createInputStream("value1\nvalue2\nvalue3"))
    reader.next() should be(None)
  }
  test("sequence number in the input returns the line") {
    val reader = new SequenceBasedLineReader(createInputStream("1value1\nvalue2\nvalue3"))
    reader.next() should be(Some("1value1\nvalue2\nvalue3"))
    reader.next() should be(None)
  }

  test("multiple lines returned") {
    val reader = new SequenceBasedLineReader(createInputStream("1value1\n2value2\n3value3"))
    reader.next() shouldBe Some("1value1")
    reader.next() shouldBe Some("2value2")
    reader.next() shouldBe Some("3value3")
    reader.next() shouldBe None
  }
  test("multiple lines spanning multiple lines in the underlying stream") {
    val reader = new SequenceBasedLineReader(createInputStream(
      """
        |1value1
        |value11
        |
        |value111
        |2value2
        |3value3
        |
        |4value4
        |value41
        |value42
        |5value5
        |
        |value51""".stripMargin,
    ))
    reader.next() shouldBe Some(
      """1value1
        |value11
        |
        |value111""".stripMargin,
    )
    reader.next() shouldBe Some("2value2")
    reader.next() shouldBe Some("3value3\n")
    reader.next() shouldBe Some(
      """4value4
        |value41
        |value42""".stripMargin,
    )
    reader.next() shouldBe Some("5value5\n\nvalue51")
  }

  private def createInputStream(str: String): InputStream = new ByteArrayInputStream(str.getBytes)

}
