package com.datamountaineer.streamreactor.connect.pulsar.sink

import com.datamountaineer.streamreactor.connect.pulsar.config.PulsarConfigConstants
import org.apache.kafka.common.config.ConfigException
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 24/01/2018. 
  * stream-reactor
  */
class PulsarSinkConnectorTest extends WordSpec with Matchers {

  val pulsarTopic = "persistent://landoop/standalone/connect/kafka-topic"

  "should start a Connector and split correctly" in {
    val props = Map(
      "topics" -> "kafka_topic",
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic BATCH = 10 WITHPARTITIONER = SinglePartition WITHCOMPRESSION = ZLIB WITHDELAY = 1000"
    ).asJava

    val connector = new PulsarSinkConnector()
    connector.start(props)
    val taskConfigs = connector.taskConfigs(1)
    taskConfigs.size shouldBe 1
    connector.taskClass() shouldBe classOf[PulsarSinkTask]
    connector.stop()
  }

  "should throw as topic doesn't match kcql" in {
    val props = Map(
      "topics" -> "bad",
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic BATCH = 10 WITHPARTITIONER =  SinglePartition WITHCOMPRESSION = ZLIB WITHDELAY = 1000"
    ).asJava

    val connector = new PulsarSinkConnector()

    intercept[ConfigException] {
      connector.start(props)
    }
  }
}
