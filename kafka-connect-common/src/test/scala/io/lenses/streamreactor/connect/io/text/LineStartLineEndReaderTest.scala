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
  private def createInputStream(data: String): InputStream = new ByteArrayInputStream(data.getBytes)
}
