package com.datamountaineer.streamreactor.connect.pulsar.config

import com.datamountaineer.kcql.Kcql
import org.apache.pulsar.client.api.MessageRoutingMode
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class KcqlMessageRoutingTest extends AnyWordSpec with Matchers with MockitoSugar {

  "should parse SINGLE type in all cases" in {
    KcqlMessageRouting(prepareMockKcql("singlepartition ")) should be (MessageRoutingMode.SinglePartition)
    KcqlMessageRouting(prepareMockKcql("SINGLEPARTITION")) should be (MessageRoutingMode.SinglePartition)
    KcqlMessageRouting(prepareMockKcql("sInGlEpArTiTiOn")) should be (MessageRoutingMode.SinglePartition)
  }

  "should parse ROUNDROBINPARTITION type in all cases" in {
    KcqlMessageRouting(prepareMockKcql("roundrobinpartition ")) should be (MessageRoutingMode.RoundRobinPartition)
    KcqlMessageRouting(prepareMockKcql("ROUNDROBINPARTITION")) should be (MessageRoutingMode.RoundRobinPartition)
    KcqlMessageRouting(prepareMockKcql("rOuNdRoBiNpArTiTiOn")) should be (MessageRoutingMode.RoundRobinPartition)
  }

  "should parse CUSTOMPARTITION type in all cases" in {
    KcqlMessageRouting(prepareMockKcql("custompartition ")) should be (MessageRoutingMode.CustomPartition)
    KcqlMessageRouting(prepareMockKcql("CUSTOMPARTITION")) should be (MessageRoutingMode.CustomPartition)
    KcqlMessageRouting(prepareMockKcql("cUsToMpArTiTiOn")) should be (MessageRoutingMode.CustomPartition)
  }

  "should default any other type to SINGLE" in {
    KcqlMessageRouting(prepareMockKcql("blob ")) should be (MessageRoutingMode.SinglePartition)
    KcqlMessageRouting(prepareMockKcql("blib")) should be (MessageRoutingMode.SinglePartition)
    KcqlMessageRouting(prepareMockKcql(null)) should be (MessageRoutingMode.SinglePartition)
  }

  private def prepareMockKcql(withPartitioner: String) = {
    val kcql = mock[Kcql]
    when(kcql.getWithPartitioner).thenReturn(withPartitioner)
    kcql
  }
}
