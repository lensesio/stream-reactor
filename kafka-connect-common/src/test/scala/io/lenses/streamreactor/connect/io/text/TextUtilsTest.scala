package io.lenses.streamreactor.connect.io.text

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TextUtilsTest extends AnyFunSuite with Matchers {
  test("partiallyEndsWith") {
    TextUtils.partiallyEndsWith("abcde", "cde") should be(Some("cde"))
    TextUtils.partiallyEndsWith("abc", "bcde") should be(Some("bc"))
    TextUtils.partiallyEndsWith("aaab", "bc") should be(Some("bc"))
    TextUtils.partiallyEndsWith("aaa\n b", "bc") should be(Some("b"))
  }
  test("partiallyEndsWith handles empty target") {
    TextUtils.partiallyEndsWith("", "cde") should be(None)
  }
  test("partiallyEndsWith handles empty source") {
    TextUtils.partiallyEndsWith("abcde", "") should be(None)
  }
}
