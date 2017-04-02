package com.datamountaineer.streamreactor.connect.source

import com.datamountaineer.streamreactor.connect.TestBase
import com.datamountaineer.streamreactor.connect.jms.config.JMSConfig
import com.datamountaineer.streamreactor.connect.jms.source.JMSSourceConnector

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 24/03/2017. 
  * stream-reactor
  */
class JMSSourceConnectorTest extends TestBase {
 "should start a JMS Source Connector" in {
   val props = getPropsMixCDI()
   val connector = new JMSSourceConnector()
   connector.start(props = props)
   val configs = connector.taskConfigs(2)
   val config1 = configs.asScala.head.asScala
   val config2 = configs.asScala.last.asScala
   config1(JMSConfig.KCQL) shouldBe KCQL_SOURCE_QUEUE
   config2(JMSConfig.KCQL) shouldBe KCQL_SOURCE_TOPIC
   connector.stop()
 }
}
