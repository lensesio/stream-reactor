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

class LineStartLineEndReaderTest extends AnyFunSuite with Matchers {
  test("handle empty input") {
    val reader = new PrefixSuffixReader(createInputStream(""), "start", "end")
    reader.next() shouldBe None
  }
  test("return none if there is no matching line = start and a following one = end") {
    val data = List(
      """
        |start
        |start
        |""".stripMargin,
      """end
        |end
        |""".stripMargin,
      "endstart",
      """
        |end
        |start
        |""".stripMargin,
    )
    data.foreach { d =>
      withClue(d) {
        val reader = new LineStartLineEndReader(createInputStream(d), "start", "end")
        reader.next() shouldBe None
      }
    }
  }
  test("one line startend returns none") {
    val reader = new LineStartLineEndReader(createInputStream("startend"), "start", "end")
    reader.next() shouldBe None
  }
  test("returns one line matching start and end lines") {
    val data = List(
      """
        |start
        |end
        |""".stripMargin,
      """
        |a
        |start
        |end
        |b
        |""".stripMargin,
      """
        |start
        |end
        |a
        |""".stripMargin,
      """
        |a
        |start
        |end
        |
        |""".stripMargin,
    )
    data.foreach { d =>
      withClue(d) {
        val reader = new LineStartLineEndReader(createInputStream(d), "start", "end")
        reader.next() shouldBe Some(
          """start
            |end""".stripMargin,
        )
      }
    }
  }

  test("multiple records found") {
    val data = List(
      """
        |start
        |end
        |start
        |end
        |""".stripMargin,
      """
        |a
        |start
        |end
        |b
        |start
        |end
        |c
        |""".stripMargin,
      """
        |start
        |end
        |a
        |start
        |end
        |b
        |""".stripMargin,
      """
        |a
        |start
        |end
        |
        |start
        |end
        |b
        |""".stripMargin,
    )
    data.foreach { d =>
      withClue(d) {
        val reader = new LineStartLineEndReader(createInputStream(d), "start", "end")
        reader.next() shouldBe Some(
          """start
            |end""".stripMargin,
        )
        reader.next() shouldBe Some(
          """start
            |end""".stripMargin,
        )
      }
    }
  }

  test("multiple records found with skip lines") {
    val data = List(
      """
        |start
        |end
        |start
        |end
        |""".stripMargin,
      """
        |a
        |start
        |end
        |b
        |start
        |end
        |c
        |""".stripMargin,
      """
        |start
        |end
        |a
        |start
        |end
        |b
        |""".stripMargin,
      """
        |a
        |start
        |end
        |
        |start
        |end
        |b
        |""".stripMargin,
    )
    data.foreach { d =>
      withClue(d) {
        val reader = new LineStartLineEndReader(createInputStream(d), "start", "end")
        reader.next() shouldBe Some(
          """start
            |end""".stripMargin,
        )
        reader.next() shouldBe Some(
          """start
            |end""".stripMargin,
        )
      }
    }
  }
  test("maintain the line separator when there are multiple lines between start and end") {
    val reader = new LineStartLineEndReader(createInputStream(
                                              """
                                                |start
                                                |a
                                                |b
                                                |c
                                                |end""".stripMargin,
                                            ),
                                            "start",
                                            "end",
    )
    reader.next() shouldBe Some(
      """start
        |a
        |b
        |c
        |end""".stripMargin,
    )
  }
  test("handle trim=true") {
    val reader = new LineStartLineEndReader(createInputStream(
                                              """
                                                |start
                                                |a
                                                |b
                                                |c
                                                |
                                                |start
                                                |x
                                                |
                                                |""".stripMargin,
                                            ),
                                            "start",
                                            "",
                                            trim = true,
    )
    reader.next() shouldBe Some(
      """start
        |a
        |b
        |c""".stripMargin,
    )
    reader.next() shouldBe Some(
      """start
        |x""".stripMargin,
    )
  }

  test("when lastEndLineMissing=true, return the record if the end line is missing") {
    val reader = new LineStartLineEndReader(createInputStream(
                                              """
                                                |start
                                                |a
                                                |b
                                                |c
                                                |
                                                |start
                                                |x""".stripMargin,
                                            ),
                                            "start",
                                            "",
                                            trim               = true,
                                            lastEndLineMissing = true,
    )
    reader.next() shouldBe Some(
      """start
        |a
        |b
        |c""".stripMargin,
    )
    reader.next() shouldBe Some(
      """start
        |x""".stripMargin,
    )
  }

  test("when lastEndLineMissing=true, return the record if the end line is missing all file is a message") {
    val reader = new LineStartLineEndReader(createInputStream(
                                              """
                                                |start
                                                |a
                                                |b
                                                |c
                                                |start
                                                |x""".stripMargin,
                                            ),
                                            "start",
                                            "",
                                            trim               = true,
                                            lastEndLineMissing = true,
    )
    reader.next() shouldBe Some(
      """start
        |a
        |b
        |c
        |start
        |x""".stripMargin,
    )
  }
  private def createInputStream(data: String): InputStream = new ByteArrayInputStream(data.getBytes)
}
