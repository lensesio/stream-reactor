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
    val reader = new PrefixSuffixLineReader(createInputStream(""), "prefix", "suffix")
    reader.next() should be(None)
  }
  test("prefix and suffix as one line") {
    val reader = new PrefixSuffixLineReader(createInputStream("prefixvalue1suffix"), "prefix", "suffix")
    reader.next() should be(Some("prefixvalue1suffix"))
    reader.next() should be(None)
  }
  test("prefix and suffix as two lines") {
    val reader = new PrefixSuffixLineReader(createInputStream("prefixvalue1\nsuffix"), "prefix", "suffix")
    reader.next() should be(Some("prefixvalue1\nsuffix"))
    reader.next() should be(None)
  }
  test("prefix and suffix as three lines") {
    val reader = new PrefixSuffixLineReader(createInputStream("prefixvalue1\nvalue2\nsuffix"), "prefix", "suffix")
    reader.next() should be(Some("prefixvalue1\nvalue2\nsuffix"))
    reader.next() should be(None)
  }
  test("prefix and suffix as three lines with lines skipped") {
    val reader =
      new PrefixSuffixLineReader(createInputStream("value0\nprefixvalue1\nvalue2\nsuffix"), "prefix", "suffix", 1)
    reader.next() should be(Some("prefixvalue1\nvalue2\nsuffix"))
    reader.next() should be(None)
  }
  test("prefix and suffix as three lines with lines skipped and no suffix") {
    val reader =
      new PrefixSuffixLineReader(createInputStream("value0\nprefixvalue1\nvalue2\nsuffix\nprefixvalue3"),
                                 "prefix",
                                 "suffix",
                                 1,
      )
    reader.next() should be(Some("prefixvalue1\nvalue2\nsuffix"))
    reader.next() should be(None)
  }

  private def createInputStream(data: String): InputStream = new ByteArrayInputStream(data.getBytes)
}
