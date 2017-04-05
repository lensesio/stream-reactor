/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
