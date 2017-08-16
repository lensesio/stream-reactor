package com.datamountaineer.streamreactor.connect.elastic.indexname

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class TestIndexNameFragment extends FlatSpec with Matchers with GeneratorDrivenPropertyChecks {

  "TextFragment" should "return the original text when using getFragment()" in {
    forAll(Gen.alphaStr) { someString =>
      TextFragment(someString).getFragment shouldBe someString
    }
  }

  "DateTimeFragment" should "return the formatted date when using getFragment()" in new ClockFixture {
    val dateTimeFormat = "YYYY-MM-dd HH:mm:ss"
    val expectedResult = "2016-10-02 14:00:00"
    DateTimeFragment(dateTimeFormat, TestClock).getFragment shouldBe expectedResult
  }
}
