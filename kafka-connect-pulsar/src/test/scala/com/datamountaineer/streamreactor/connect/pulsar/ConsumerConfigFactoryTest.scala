package com.datamountaineer.streamreactor.connect.pulsar

import com.datamountaineer.streamreactor.connect.pulsar.config._
import org.apache.pulsar.client.api.SubscriptionType
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 23/01/2018. 
  * stream-reactor
  */
class ConsumerConfigFactoryTest extends WordSpec with Matchers {
  val pulsarTopic = "persistent://landoop/standalone/connect/kafka-topic"

  "should create a config with batch settings" in {

    val config = PulsarSourceConfig(Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10",
      PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
      PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
    ).asJava)


    val settings = PulsarSourceSettings(config, 1)
    val consumerConfig = ConsumerConfigFactory("test", settings.kcql)
    consumerConfig(pulsarTopic).getReceiverQueueSize shouldBe 10
    consumerConfig(pulsarTopic).getConsumerName shouldBe "test"
  }

  "should create a config with Failover mode" in {

    val config = PulsarSourceConfig(Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10 WITHSUBSCRIPTION = failOver",
      PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
      PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
    ).asJava)


    val settings = PulsarSourceSettings(config, 2)
    val consumerConfig = ConsumerConfigFactory("test", settings.kcql)
    consumerConfig(pulsarTopic).getReceiverQueueSize shouldBe 10
    consumerConfig(pulsarTopic).getConsumerName shouldBe "test"
    consumerConfig(pulsarTopic).getSubscriptionType shouldBe SubscriptionType.Failover
  }

  "should create a config with exclusive mode" in {
    val config = PulsarSourceConfig(Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10 WITHSUBSCRIPTION = Exclusive",
      PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
      PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
    ).asJava)


    val settings = PulsarSourceSettings(config, 1)
    val consumerConfig = ConsumerConfigFactory("test", settings.kcql)
    consumerConfig(pulsarTopic).getReceiverQueueSize shouldBe 10
    consumerConfig(pulsarTopic).getConsumerName shouldBe "test"
    consumerConfig(pulsarTopic).getSubscriptionType shouldBe SubscriptionType.Exclusive

  }

  "should create a config with shared mode" in {
    val config = PulsarSourceConfig(Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10 WITHSUBSCRIPTION = shared",
      PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
      PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
    ).asJava)


    val settings = PulsarSourceSettings(config, 2)
    val consumerConfig = ConsumerConfigFactory("test", settings.kcql)
    consumerConfig(pulsarTopic).getReceiverQueueSize shouldBe 10
    consumerConfig(pulsarTopic).getConsumerName shouldBe "test"
    consumerConfig(pulsarTopic).getSubscriptionType shouldBe SubscriptionType.Shared
  }

}
