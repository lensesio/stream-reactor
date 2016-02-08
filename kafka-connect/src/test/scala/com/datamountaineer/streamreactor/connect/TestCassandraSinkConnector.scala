package com.datamountaineer.streamreactor.connect

import scala.collection.JavaConverters._

class TestCassandraSinkConnector extends TestConfigUtils {
  test("Should start a Cassandra Connector") {
    val config = getCassandraSinkConfigProps
    val connector = new CassandraSinkConnector()
    connector.start(config)
    val taskConfigs = connector.taskConfigs(10)
    taskConfigs.asScala.head.get(CassandraSinkConfig.CONTACT_POINTS) shouldBe CONTACT_POINT
    taskConfigs.asScala.head.get(CassandraSinkConfig.KEY_SPACE) shouldBe TABLE
    taskConfigs.size() shouldBe 10
    connector.taskClass() shouldBe classOf[CassandraSinkTask]
    connector.version() shouldBe ""
    connector.stop()
  }
}
