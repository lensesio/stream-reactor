package com.datamountaineer.streamreactor.connect.cassandra.sink

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigConstants
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._

class TestCassandraSinkConnector extends WordSpec with BeforeAndAfter with Matchers with TestConfig {
  "Should start a Cassandra Sink Connector" in {
    val props = getCassandraConfigSinkPropsSecure
    val connector = new CassandraSinkConnector()
    connector.start(props)
    val taskConfigs = connector.taskConfigs(1)
    taskConfigs.asScala.head.get(CassandraConfigConstants.EXPORT_TABLE_TOPIC_MAP) shouldBe EXPORT_TOPIC_TABLE_MAP
    taskConfigs.asScala.head.get(CassandraConfigConstants.CONTACT_POINTS) shouldBe CONTACT_POINT
    taskConfigs.asScala.head.get(CassandraConfigConstants.KEY_SPACE) shouldBe TOPIC1
    taskConfigs.size() shouldBe 1
    connector.taskClass() shouldBe classOf[CassandraSinkTask]
    //connector.version() shouldBe ""
    connector.stop()
  }
}
