package com.datamountaineer.streamreactor.connect.pulsar.source

import com.datamountaineer.streamreactor.connect.pulsar.config.PulsarConfigConstants
import org.apache.kafka.common.config.ConfigException
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 24/01/2018. 
  * stream-reactor
  */
class PulsarSourceConnectorTest extends WordSpec with Matchers {

  val pulsarTopic = "persistent://landoop/standalone/connect/kafka-topic"
  val pulsarTopic1 = "persistent://landoop/standalone/connect/kafka-topic1"

  "should start a connector with shared consumer" in {
    val kcql = s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10 WITHSUBSCRIPTION = SHARED"
    val kcql1 = s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic1 BATCH = 10 WITHSUBSCRIPTION = FAILOVER"

    val props =  Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"$kcql;$kcql1",
      PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
      PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
    ).asJava

    val connector = new PulsarSourceConnector()
    connector.start(props)
    val configs = connector.taskConfigs(2)
    configs.size() shouldBe 2
    connector.taskClass() shouldBe classOf[PulsarSourceTask]
  }

  "should fail to start a connector with exclusive consumer and more than one task" in {
    val props =  Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10 WITHSUBSCRIPTION = exclusive;INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10 WITHSUBSCRIPTION = FAILOVER",
      PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
      PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
    ).asJava

    val connector = new PulsarSourceConnector()
    connector.start(props)
    intercept[ConfigException] {
      connector.taskConfigs(2)
    }
  }
}
