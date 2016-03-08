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
