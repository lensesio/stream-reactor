package com.datamountaineer.streamreactor.connect.kudu

import scala.collection.JavaConverters._
/**
  * Created by andrew@datamountaineer.com on 24/02/16. 
  * stream-reactor
  */
class TestKuduSinkConnector extends TestBase {
  test("Should start a Kudu Connector") {
    val config = getConfig
    val connector = new KuduSinkConnector()
    connector.start(config)
    val taskConfigs = connector.taskConfigs(1)
    taskConfigs.asScala.head.get(KuduSinkConfig.KUDU_MASTER) shouldBe KUDU_MASTER
    taskConfigs.size() shouldBe 1
    connector.taskClass() shouldBe classOf[KuduSinkTask]
    connector.stop()
  }
}
