package com.datamountaineer.streamreactor.connect

class TestCassandraSinkConnector extends TestConfigUtils {
  test("Should start a Cassandra Connector") {
    val config = getCassandraSinkConfigProps()
    val connector = new CassandraSinkConnector()
    connector.start(config)
    val taskConfigs = connector.taskConfigs(10)
    taskConfigs.size() shouldBe(10)
    connector.taskClass() shouldBe(classOf[CassandraSinkTask])
  }

}
