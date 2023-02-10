package io.lenses.streamreactor.connect.aws.s3.sink

import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class PaddingStrategyTest extends AnyFlatSpecLike with Matchers {

  "NoOpPaddingStrategy" should "return string as is" in {
    NoOpPaddingStrategy.padString("1") should be("1")
  }

  "LeftPaddingStrategy" should "pad string left" in {
    LeftPadPaddingStrategy(5, '0').padString("2") should be("00002")
  }

  "RightPaddingStrategy" should "pad string right" in {
    RightPadPaddingStrategy(10, '0').padString("3") should be("3000000000")
  }
}
