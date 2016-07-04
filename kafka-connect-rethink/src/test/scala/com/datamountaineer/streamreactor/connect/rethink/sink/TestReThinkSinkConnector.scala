package com.datamountaineer.streamreactor.connect.rethink.sink

import com.datamountaineeer.streamreactor.connect.rethink.config.ReThinkSinkConfig
import com.datamountaineeer.streamreactor.connect.rethink.sink.{ReThinkSinkConnector, ReThinkSinkTask}
import com.datamountaineer.streamreactor.connect.rethink.TestBase
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 22/06/16. 
  * stream-reactor-maven
  */
class TestReThinkSinkConnector extends TestBase {
  "Should start a ReThink Connector" in {
    val config = getProps
    val connector = new ReThinkSinkConnector()
    connector.start(config)
    val taskConfigs = connector.taskConfigs(1)
    taskConfigs.asScala.head.get(ReThinkSinkConfig.EXPORT_ROUTE_QUERY) shouldBe ROUTE
    taskConfigs.size() shouldBe 1
    connector.taskClass() shouldBe classOf[ReThinkSinkTask]
    connector.stop()
  }
}
