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

package com.datamountaineer.streamreactor.connect.bloomberg

import org.scalatest.{Matchers, WordSpec}

class SubscriptionInfoExtractFnTest extends WordSpec with Matchers {
  "SubscriptionInfoExtractFn" should {
    "handle empty settings" in {
      intercept[IllegalArgumentException] {
        SubscriptionInfoExtractFn("") shouldBe Seq.empty
      }
    }

    "handle one ticker subscription" in {
      SubscriptionInfoExtractFn("ticker1: field1, field2, field3") shouldBe Seq(
        SubscriptionInfo("ticker1", List("FIELD1", "FIELD2", "FIELD3"))
      )
    }
    "handle multiple tickers subscription" in {
      SubscriptionInfoExtractFn("ticker1: field1, field2, field3; ticker2:field1;ticker3:fieldA") shouldBe List(
        SubscriptionInfo("ticker1", List("FIELD1", "FIELD2", "FIELD3")),
        SubscriptionInfo("ticker2", List("FIELD1")),
        SubscriptionInfo("ticker3", List("FIELDA"))
      )
    }
    "handle missing : between ticker and fields" in {
      intercept[IllegalArgumentException] {
        SubscriptionInfoExtractFn("ticker field1, field2, field3")
      }
    }

    "handle missing fields for a ticker subscription" in {
      intercept[IllegalArgumentException] {
        SubscriptionInfoExtractFn("ticker1:f1,f2;ticker2:")
      }
    }
  }
}