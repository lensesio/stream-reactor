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

import com.bloomberglp.blpapi.{CorrelationID, Subscription, SubscriptionList}
import org.scalatest.{Matchers, WordSpec}

class CorrelationIdsExtractorFnTest extends WordSpec with Matchers {
  "CorrelationIdsExtractorFn" should {
    "handle null parameter" in {
      CorrelationIdsExtractorFn(null) shouldBe ""
    }
    "list all the correlation ids" in {
      val subscriptions = new SubscriptionList()
      subscriptions.add(new Subscription("someticker1", new CorrelationID(11)))
      subscriptions.add(new Subscription("someticker2", new CorrelationID(31)))
      CorrelationIdsExtractorFn(subscriptions) shouldBe "11,31"
    }
  }
}
