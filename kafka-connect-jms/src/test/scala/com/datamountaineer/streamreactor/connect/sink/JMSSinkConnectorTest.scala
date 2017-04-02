package com.datamountaineer.streamreactor.connect.sink

import com.datamountaineer.streamreactor.connect.TestBase
import com.datamountaineer.streamreactor.connect.jms.sink.JMSSinkConnector

/**
  * Created by andrew@datamountaineer.com on 24/03/2017. 
  * stream-reactor
  */
class JMSSinkConnectorTest extends TestBase {
  "should start a JMSSinkConnector" in {
    val props = getProps1Queue()
    val connector = new JMSSinkConnector()
    connector.start(props)
    val configs = connector.taskConfigs(1)
    configs.size() shouldBe 1
  }
}
