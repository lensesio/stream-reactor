package com.datamountaineer.streamreactor.connect.pulsar

import com.datamountaineer.streamreactor.connect.pulsar.config.{PulsarConfigConstants, PulsarSinkConfig, PulsarSinkSettings}
import org.apache.pulsar.client.api.CompressionType
import org.apache.pulsar.client.api.ProducerConfiguration.MessageRoutingMode
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 23/01/2018. 
  * stream-reactor
  */
class ProducerConfigFactoryTest extends WordSpec with Matchers {

  val pulsarTopic = "persistent://landoop/standalone/connect/kafka-topic"

  "should create a SinglePartition with batching" in {
    val config = PulsarSinkConfig(Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic BATCH = 10 WITHPARTITIONER = SinglePartition WITHCOMPRESSION = ZLIB WITHDELAY = 1000"
    ).asJava)


    val settings = PulsarSinkSettings(config)
    val producerConfig = ProducerConfigFactory("test", settings.kcql)
    producerConfig(pulsarTopic).getBatchingEnabled shouldBe true
    producerConfig(pulsarTopic).getBatchingMaxMessages shouldBe 10
    producerConfig(pulsarTopic).getBatchingMaxPublishDelayMs shouldBe 1000

    producerConfig(pulsarTopic).getCompressionType shouldBe CompressionType.ZLIB
    producerConfig(pulsarTopic).getMessageRoutingMode shouldBe MessageRoutingMode.SinglePartition
  }

  "should create a CustomPartition with no batching and no compression" in {
    val config = PulsarSinkConfig(Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic WITHPARTITIONER = CustomPartition"
    ).asJava)


    val settings = PulsarSinkSettings(config)
    val producerConfig = ProducerConfigFactory("test", settings.kcql)
    producerConfig(pulsarTopic).getBatchingEnabled shouldBe false
    producerConfig(pulsarTopic).getCompressionType shouldBe CompressionType.NONE
    producerConfig(pulsarTopic).getMessageRoutingMode shouldBe MessageRoutingMode.CustomPartition
  }

  "should create a roundrobin with batching and no compression no delay" in {
    val config = PulsarSinkConfig(Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic BATCH  = 10 WITHPARTITIONER = ROUNDROBINPARTITION"
    ).asJava)


    val settings = PulsarSinkSettings(config)
    val producerConfig = ProducerConfigFactory("test", settings.kcql)
    producerConfig(pulsarTopic).getBatchingEnabled shouldBe true
    producerConfig(pulsarTopic).getBatchingEnabled shouldBe true
    producerConfig(pulsarTopic).getBatchingMaxMessages shouldBe 10
    producerConfig(pulsarTopic).getBatchingMaxPublishDelayMs shouldBe 10

    producerConfig(pulsarTopic).getCompressionType shouldBe CompressionType.NONE
    producerConfig(pulsarTopic).getMessageRoutingMode shouldBe MessageRoutingMode.RoundRobinPartition
  }
}
