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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.io.InputStream

class PrefixSuffixLineReaderTest extends AnyFunSuite with Matchers {
  test("empty input stream returns None") {
    val reader = new PrefixSuffixLineReader(createInputStream(""), "prefix", "suffix", 0, false)
    reader.next() should be(None)
  }
  test("prefix and suffix as one line") {
    val reader = new PrefixSuffixLineReader(createInputStream("prefixvalue1suffix"), "prefix", "suffix", 0, false)
    reader.next() should be(Some("prefixvalue1suffix"))
    reader.next() should be(None)
  }
  test("prefix and suffix as two lines") {
    val reader = new PrefixSuffixLineReader(createInputStream("prefixvalue1\nsuffix"), "prefix", "suffix", 0, false)
    reader.next() should be(Some("prefixvalue1\nsuffix"))
    reader.next() should be(None)
  }
  test("prefix and suffix as three lines") {
    val reader =
      new PrefixSuffixLineReader(createInputStream("prefixvalue1\nvalue2\nsuffix"), "prefix", "suffix", 0, false)
    reader.next() should be(Some("prefixvalue1\nvalue2\nsuffix"))
    reader.next() should be(None)
  }
  test("prefix and suffix as three lines with lines skipped") {
    val reader =
      new PrefixSuffixLineReader(createInputStream("value0\nprefixvalue1\nvalue2\nsuffix"),
                                 "prefix",
                                 "suffix",
                                 1,
                                 false,
      )
    reader.next() should be(Some("prefixvalue1\nvalue2\nsuffix"))
    reader.next() should be(None)
  }
  test("prefix and suffix as three lines with lines skipped and no suffix") {
    val reader =
      new PrefixSuffixLineReader(createInputStream("value0\nprefixvalue1\nvalue2\nsuffix\nprefixvalue3"),
                                 "prefix",
                                 "suffix",
                                 1,
                                 false,
      )
    reader.next() should be(Some("prefixvalue1\nvalue2\nsuffix"))
    reader.next() should be(None)
  }
  test("multiple records on the same line") {
    val reader =
      new PrefixSuffixLineReader(createInputStream("prefixvalue1suffixprefixvalue2suffix"),
                                 "prefix",
                                 "suffix",
                                 0,
                                 false,
      )
    reader.next() should be(Some("prefixvalue1suffix"))
    reader.next() should be(Some("prefixvalue2suffix"))
    reader.next() should be(None)
  }
  test("multiple records on the same line and multiple lines") {
    val reader =
      new PrefixSuffixLineReader(createInputStream("prefixvalue1suffixprefixvalue2suffix\nprefixvalue3suffix"),
                                 "prefix",
                                 "suffix",
                                 0,
                                 false,
      )
    reader.next() should be(Some("prefixvalue1suffix"))
    reader.next() should be(Some("prefixvalue2suffix"))
    reader.next() should be(Some("prefixvalue3suffix"))
    reader.next() should be(None)
  }
  test("multiple records on the same line, last record spanning to the next line") {
    val reader =
      new PrefixSuffixLineReader(createInputStream("prefixvalue1suffix\nprefixvalue2suffixprefix\nvalue3suffix"),
                                 "prefix",
                                 "suffix",
                                 0,
                                 false,
      )
    reader.next() should be(Some("prefixvalue1suffix"))
    reader.next() should be(Some("prefixvalue2suffix"))
    reader.next() should be(Some("prefix\nvalue3suffix"))
    reader.next() should be(None)
  }
  test(
    "multiple records on the same line, last record spanning to the next 2 lines, then more records on the same line",
  ) {
    val reader = new PrefixSuffixLineReader(
      createInputStream("prefixvalue1suffix\nprefixvalue2suffixprefix\nvalue3suffixprefixvalue4suffix"),
      "prefix",
      "suffix",
      0,
      false,
    )
    reader.next() should be(Some("prefixvalue1suffix"))
    reader.next() should be(Some("prefixvalue2suffix"))
    reader.next() should be(Some("prefix\nvalue3suffix"))
    reader.next() should be(Some("prefixvalue4suffix"))
    reader.next() should be(None)
  }
  test("prefix found but not suffix over multiple lines input") {
    val reader =
      new PrefixSuffixLineReader(createInputStream("prefixvalue1\nvalue2\nvalue3"), "prefix", "suffix", 0, false)
    reader.next() should be(None)
  }
  test("prefix found but not suffix over multiple lines input with lines skipped") {
    val reader =
      new PrefixSuffixLineReader(createInputStream("value0\nprefixvalue1\nvalue2\nvalue3"),
                                 "prefix",
                                 "suffix",
                                 1,
                                 false,
      )
    reader.next() should be(None)
  }
  test("record returned before no suffix is found") {
    val reader = new PrefixSuffixLineReader(createInputStream("prefixvalue1\nvalue2\nvalue3\nsuffixprefixvalue4"),
                                            "prefix",
                                            "suffix",
                                            0,
                                            false,
    )
    reader.next() should be(Some("prefixvalue1\nvalue2\nvalue3\nsuffix"))
    reader.next() should be(None)
  }
  test("record returned before the content remaining does not have a prefix only suffix with lines skipped") {
    val reader = new PrefixSuffixLineReader(
      createInputStream("value0\nprefixvalue1\nvalue2\nvalue3\nvalue4\nsuffix\nvalue5suffix"),
      "prefix",
      "suffix",
      1,
      false,
    )
    reader.next() should be(Some("prefixvalue1\nvalue2\nvalue3\nvalue4\nsuffix"))
    reader.next() should be(None)
  }
  test("record returned before the content has suffix, and then another record is returned") {
    val reader = new PrefixSuffixLineReader(createInputStream(
                                              """
                                                |prefixvalue1
                                                |value2
                                                |value3
                                                |prefixvalue4
                                                |suffix
                                                |value5suffix
                                                |prefixvalue6suffix""".stripMargin,
                                            ),
                                            "prefix",
                                            "suffix",
                                            0,
                                            false,
    )
    reader.next() should be(Some("prefixvalue1\nvalue2\nvalue3\nprefixvalue4\nsuffix"))
    reader.next() should be(Some("prefixvalue6suffix"))
    reader.next() should be(None)
  }
  test("validate skip argument being positive") {
    intercept[IllegalArgumentException] {
      new PrefixSuffixLineReader(createInputStream("prefixvalue1\nvalue2\nvalue3\nsuffixprefixvalue4"),
                                 "prefix",
                                 "suffix",
                                 -1,
                                 false,
      )
    }
  }
  test("multiple records but skip argument is too large") {
    val reader =
      new PrefixSuffixLineReader(createInputStream("prefixvalue1suffixprefixvalue2suffix"),
                                 "prefix",
                                 "suffix",
                                 3,
                                 false,
      )
    reader.next() should be(None)
  }
  test("handle Instructor xml tags") {
    val reader =
      new PrefixSuffixLineReader(createInputStream(
                                   """"
                                     |<?xml version="1.0" encoding="utf-8"?>
                                     |<InstructorPayLevel>
                                     |    <Instructor>
                                     |        <EmployeeNumber>188</EmployeeNumber>
                                     |        <PayLevel>11</PayLevel>
                                     |        <BidPeriod>22-06</BidPeriod>
                                     |        <PayLevelDescription>Description1</PayLevelDescription>
                                     |        <TrainingInstructorCategory>Category1</TrainingInstructorCategory>
                                     |        <DataSource>PR</DataSource>
                                     |    </Instructor>
                                     |    <Instructor>
                                     |        <EmployeeNumber>173</EmployeeNumber>
                                     |        <PayLevel>11</PayLevel>
                                     |        <BidPeriod>22-06</BidPeriod>
                                     |        <PayLevelDescription>1485$ level  APD</PayLevelDescription>
                                     |        <TrainingInstructorCategory>Category2</TrainingInstructorCategory>
                                     |        <DataSource>PR</DataSource>
                                     |    </Instructor>
                                     | </InstructorPayLevel>
                                     |""".stripMargin,
                                 ),
                                 "<Instructor>",
                                 "</Instructor>",
                                 0,
                                 false,
      )
    reader.next() shouldBe Some(
      """<Instructor>
        |        <EmployeeNumber>188</EmployeeNumber>
        |        <PayLevel>11</PayLevel>
        |        <BidPeriod>22-06</BidPeriod>
        |        <PayLevelDescription>Description1</PayLevelDescription>
        |        <TrainingInstructorCategory>Category1</TrainingInstructorCategory>
        |        <DataSource>PR</DataSource>
        |    </Instructor>""".stripMargin,
    )
    reader.next() shouldBe Some(
      """<Instructor>
        |        <EmployeeNumber>173</EmployeeNumber>
        |        <PayLevel>11</PayLevel>
        |        <BidPeriod>22-06</BidPeriod>
        |        <PayLevelDescription>1485$ level  APD</PayLevelDescription>
        |        <TrainingInstructorCategory>Category2</TrainingInstructorCategory>
        |        <DataSource>PR</DataSource>
        |    </Instructor>""".stripMargin,
    )
    reader.next() shouldBe None
  }
  test("handle Instructor xml tags while trimming content is enabled") {
    val reader =
      new PrefixSuffixLineReader(createInputStream(
                                   """"
                                     |<?xml version="1.0" encoding="utf-8"?>
                                     |<InstructorPayLevel>
                                     |    <Instructor>
                                     |        <EmployeeNumber>188</EmployeeNumber>
                                     |        <PayLevel>11</PayLevel>
                                     |        <BidPeriod>22-06</BidPeriod>
                                     |        <PayLevelDescription>Description1</PayLevelDescription>
                                     |        <TrainingInstructorCategory>Category1</TrainingInstructorCategory>
                                     |        <DataSource>PR</DataSource>
                                     |    </Instructor>
                                     |    <Instructor>
                                     |        <EmployeeNumber>173</EmployeeNumber>
                                     |        <PayLevel>11</PayLevel>
                                     |        <BidPeriod>22-06</BidPeriod>
                                     |        <PayLevelDescription>1485$ level  APD</PayLevelDescription>
                                     |        <TrainingInstructorCategory>Category2</TrainingInstructorCategory>
                                     |        <DataSource>PR</DataSource>
                                     |    </Instructor>
                                     | </InstructorPayLevel>
                                     |""".stripMargin,
                                 ),
                                 "<Instructor>",
                                 "</Instructor>",
                                 0,
                                 true,
      )

    reader.next() shouldBe Some(
      "<Instructor><EmployeeNumber>188</EmployeeNumber><PayLevel>11</PayLevel><BidPeriod>22-06</BidPeriod><PayLevelDescription>Description1</PayLevelDescription><TrainingInstructorCategory>Category1</TrainingInstructorCategory><DataSource>PR</DataSource></Instructor>".strip(),
    )
    reader.next() shouldBe Some(
      "<Instructor><EmployeeNumber>173</EmployeeNumber><PayLevel>11</PayLevel><BidPeriod>22-06</BidPeriod><PayLevelDescription>1485$ level  APD</PayLevelDescription><TrainingInstructorCategory>Category2</TrainingInstructorCategory><DataSource>PR</DataSource></Instructor>".strip(),
    )
  }
  private def createInputStream(data: String): InputStream = new ByteArrayInputStream(data.getBytes)
}
