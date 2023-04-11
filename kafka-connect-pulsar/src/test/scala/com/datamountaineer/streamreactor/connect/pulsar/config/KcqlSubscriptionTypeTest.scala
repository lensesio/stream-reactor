/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
