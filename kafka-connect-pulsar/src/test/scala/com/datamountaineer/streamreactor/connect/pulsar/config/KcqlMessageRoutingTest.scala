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
import org.apache.pulsar.client.api.MessageRoutingMode
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class KcqlMessageRoutingTest extends AnyWordSpec with Matchers with MockitoSugar {

  "should parse SINGLE type in all cases" in {
    KcqlMessageRouting(prepareMockKcql("singlepartition ")) should be(MessageRoutingMode.SinglePartition)
    KcqlMessageRouting(prepareMockKcql("SINGLEPARTITION")) should be(MessageRoutingMode.SinglePartition)
    KcqlMessageRouting(prepareMockKcql("sInGlEpArTiTiOn")) should be(MessageRoutingMode.SinglePartition)
  }

  "should parse ROUNDROBINPARTITION type in all cases" in {
    KcqlMessageRouting(prepareMockKcql("roundrobinpartition ")) should be(MessageRoutingMode.RoundRobinPartition)
    KcqlMessageRouting(prepareMockKcql("ROUNDROBINPARTITION")) should be(MessageRoutingMode.RoundRobinPartition)
    KcqlMessageRouting(prepareMockKcql("rOuNdRoBiNpArTiTiOn")) should be(MessageRoutingMode.RoundRobinPartition)
  }

  "should parse CUSTOMPARTITION type in all cases" in {
    KcqlMessageRouting(prepareMockKcql("custompartition ")) should be(MessageRoutingMode.CustomPartition)
    KcqlMessageRouting(prepareMockKcql("CUSTOMPARTITION")) should be(MessageRoutingMode.CustomPartition)
    KcqlMessageRouting(prepareMockKcql("cUsToMpArTiTiOn")) should be(MessageRoutingMode.CustomPartition)
  }

  "should default any other type to SINGLE" in {
    KcqlMessageRouting(prepareMockKcql("blob ")) should be(MessageRoutingMode.SinglePartition)
    KcqlMessageRouting(prepareMockKcql("blib")) should be(MessageRoutingMode.SinglePartition)
    KcqlMessageRouting(prepareMockKcql(null)) should be(MessageRoutingMode.SinglePartition)
  }

  private def prepareMockKcql(withPartitioner: String) = {
    val kcql = mock[Kcql]
    when(kcql.getWithPartitioner).thenReturn(withPartitioner)
    kcql
  }
}
