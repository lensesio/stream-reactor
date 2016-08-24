package com.datamountaineer.streamreactor.connect.hazelcast.sink

import com.datamountaineer.streamreactor.connect.hazelcast.TestBase
import com.datamountaineer.streamreactor.connect.hazelcast.config.HazelCastSinkConfig

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 10/08/16. 
  * stream-reactor
  */
class TestHazelCastSinkConnector extends TestBase {
  "should start a Hazelcast sink connector" in {
    val props = getProps
    val connector = new HazelCastSinkConnector
    connector.start(props)
    val taskConfigs = connector.taskConfigs(1)
    taskConfigs.asScala.head.get(HazelCastSinkConfig.EXPORT_ROUTE_QUERY) shouldBe EXPORT_MAP
    taskConfigs.asScala.head.get(HazelCastSinkConfig.SINK_GROUP_NAME) shouldBe GROUP_NAME
    connector.stop()
  }
}
