package com.datamountaineer.streamreactor.connect.hazelcast

import com.datamountaineer.streamreactor.connect.hazelcast.config.{HazelCastSinkConfig, HazelCastSinkSettings}
import com.hazelcast.config.Config
import com.hazelcast.core.{Hazelcast, HazelcastInstance}

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 10/08/16. 
  * stream-reactor
  */
class TestHazelCastConnection extends TestBase {
  "should connect to a Hazelcast cluster" in {
    start
    val props = getProps
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)
    val conn = HazelCastConnection(settings.connConfig)
    conn.isInstanceOf[HazelcastInstance] shouldBe true
    val connectedClients = instance.get.getClientService.getConnectedClients.toSet
    connectedClients.size shouldBe 1
    connectedClients.head.getSocketAddress.getHostName shouldBe "localhost"
    conn.shutdown()
    stop
  }
}
