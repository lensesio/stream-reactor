package com.datamountaineer.streamreactor.connect.voltdb.writers

import io.confluent.common.config.ConfigException
import org.scalatest.{Matchers, WordSpec}

class RetriesTest extends WordSpec with Matchers with Retries {
  "Retries" should {
    "return the value when no error is encountered" in {
      val expected = "The return value"
      val actual = withRetries(10, 10, Some("abcd"))(expected)
      actual shouldBe expected
    }
    "return the value if an error is thrown but max retries is not met" in {
      val expected = "The return value"
      var count = 10
      val actual = withRetries(10, 10, Some("abcd")) {
        count -= 1
        if (count == 0) expected
        else throw new RuntimeException("something went wrong")
      }

      actual shouldBe expected
    }
    "return the value even with 0 retries" in {
      val expected = 12315L
      val actual = withRetries(0, 10, Some("abcd"))(expected)
      actual shouldBe expected
    }

    "throws the last exception" in {
      var count = 4
      intercept[ConfigException] {
        withRetries(4, 10, Some("abcd")) {
          count -= 1
          if (count > 0) sys.error("Not yet")
          else throw new ConfigException("this one")
        }
      }
    }
  }
}
