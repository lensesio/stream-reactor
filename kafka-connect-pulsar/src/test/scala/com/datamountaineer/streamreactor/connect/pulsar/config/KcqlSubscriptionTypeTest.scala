package com.datamountaineer.streamreactor.connect.pulsar.config

import com.datamountaineer.kcql.Kcql
import org.apache.pulsar.client.api.SubscriptionType
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class KcqlSubscriptionTypeTest extends AnyWordSpec with Matchers with MockitoSugar {

  "should parse EXCLUSIVE type in all cases" in {
    KcqlSubscriptionType(prepareMockKcql("exclusive ")) should be(SubscriptionType.Exclusive)
    KcqlSubscriptionType(prepareMockKcql("EXCLUSIVE")) should be(SubscriptionType.Exclusive)
    KcqlSubscriptionType(prepareMockKcql("eXcLuSiVe")) should be(SubscriptionType.Exclusive)
  }

  "should parse FAILOVER type in all cases" in {
    KcqlSubscriptionType(prepareMockKcql("failover ")) should be(SubscriptionType.Failover)
    KcqlSubscriptionType(prepareMockKcql("FAILOVER")) should be(SubscriptionType.Failover)
    KcqlSubscriptionType(prepareMockKcql("fAiLoVeR")) should be(SubscriptionType.Failover)
  }
  "should parse SHARED type in all cases" in {
    KcqlSubscriptionType(prepareMockKcql("shared ")) should be(SubscriptionType.Shared)
    KcqlSubscriptionType(prepareMockKcql("SHARED")) should be(SubscriptionType.Shared)
    KcqlSubscriptionType(prepareMockKcql("sHaReD")) should be(SubscriptionType.Shared)
  }

  "should default any other type to FAILOVER" in {
    KcqlSubscriptionType(prepareMockKcql("blob ")) should be(SubscriptionType.Failover)
    KcqlSubscriptionType(prepareMockKcql("blib")) should be(SubscriptionType.Failover)
    KcqlSubscriptionType(prepareMockKcql(null)) should be(SubscriptionType.Failover)
  }

  private def prepareMockKcql(withSubscription: String) = {
    val kcql = mock[Kcql]
    when(kcql.getWithSubscription).thenReturn(withSubscription)
    kcql
  }
}
