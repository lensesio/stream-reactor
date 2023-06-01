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
      new PrefixSuffixLineReader(createInputStream("value0\nprefixvalue1\nvalue2\nsuffix\nprefixvalue3"), "prefix", "suffix", 1)
    reader.next() should be(Some("prefixvalue1\nvalue2\nsuffix"))
    reader.next() should be(None)
  }

  private def createInputStream(data: String): InputStream = new ByteArrayInputStream(data.getBytes)
}
