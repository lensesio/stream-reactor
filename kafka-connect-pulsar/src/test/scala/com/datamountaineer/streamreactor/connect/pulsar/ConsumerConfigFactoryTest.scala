package com.datamountaineer.streamreactor.connect.pulsar

import com.datamountaineer.streamreactor.connect.pulsar.config._
import org.apache.pulsar.client.api.ConsumerBuilder
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.SubscriptionType
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.MapHasAsJava

/**
  * Created by andrew@datamountaineer.com on 23/01/2018.
  * stream-reactor
  */
class ConsumerConfigFactoryTest extends AnyWordSpec with Matchers with MockitoSugar with BeforeAndAfter {
  val pulsarClient = mock[PulsarClient]
  val pulsarTopic  = "persistent://landoop/standalone/connect/kafka-topic"

  val consumerBuilder = mock[ConsumerBuilder[Array[Byte]]]
  val consumerFactory = new ConsumerConfigFactory(pulsarClient)

  before {
    reset(pulsarClient, consumerBuilder)

    when(pulsarClient.newConsumer()).thenReturn(consumerBuilder)

  }

  "should create a config with batch settings" in {

    val config = PulsarSourceConfig(
      Map(
        PulsarConfigConstants.HOSTS_CONFIG                   -> "pulsar://localhost:6650",
        PulsarConfigConstants.KCQL_CONFIG                    -> s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10",
        PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
        PulsarConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
      ).asJava,
    )

    val settings       = PulsarSourceSettings(config, 1)
    val consumerConfig = consumerFactory("test", settings.kcql)

    verify(pulsarClient, times(1)).newConsumer()

    verify(consumerConfig(pulsarTopic)).receiverQueueSize(10)
    verify(consumerConfig(pulsarTopic)).consumerName("test")
  }

  "should create a config with Failover mode" in {

    val config = PulsarSourceConfig(
      Map(
        PulsarConfigConstants.HOSTS_CONFIG                   -> "pulsar://localhost:6650",
        PulsarConfigConstants.KCQL_CONFIG                    -> s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10 WITHSUBSCRIPTION = failOver",
        PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
        PulsarConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
      ).asJava,
    )

    val settings       = PulsarSourceSettings(config, 2)
    val consumerConfig = consumerFactory("test", settings.kcql)

    verify(pulsarClient, times(1)).newConsumer()

    verify(consumerConfig(pulsarTopic)).receiverQueueSize(10)
    verify(consumerConfig(pulsarTopic)).consumerName("test")
    verify(consumerConfig(pulsarTopic)).subscriptionType(SubscriptionType.Failover)

  }

  "should create a config with exclusive mode" in {
    val config = PulsarSourceConfig(
      Map(
        PulsarConfigConstants.HOSTS_CONFIG                   -> "pulsar://localhost:6650",
        PulsarConfigConstants.KCQL_CONFIG                    -> s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10 WITHSUBSCRIPTION = Exclusive",
        PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
        PulsarConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
      ).asJava,
    )

    val settings       = PulsarSourceSettings(config, 1)
    val consumerConfig = consumerFactory("test", settings.kcql)

    verify(consumerConfig(pulsarTopic)).receiverQueueSize(10)
    verify(consumerConfig(pulsarTopic)).consumerName("test")
    verify(consumerConfig(pulsarTopic)).subscriptionType(SubscriptionType.Exclusive)

  }

  "should create a config with shared mode" in {
    val config = PulsarSourceConfig(
      Map(
        PulsarConfigConstants.HOSTS_CONFIG                   -> "pulsar://localhost:6650",
        PulsarConfigConstants.KCQL_CONFIG                    -> s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10 WITHSUBSCRIPTION = shared",
        PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
        PulsarConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
      ).asJava,
    )

    val settings       = PulsarSourceSettings(config, 2)
    val consumerConfig = consumerFactory("test", settings.kcql)

    verify(consumerConfig(pulsarTopic)).receiverQueueSize(10)
    verify(consumerConfig(pulsarTopic)).consumerName("test")
    verify(consumerConfig(pulsarTopic)).subscriptionType(SubscriptionType.Shared)

  }

}
