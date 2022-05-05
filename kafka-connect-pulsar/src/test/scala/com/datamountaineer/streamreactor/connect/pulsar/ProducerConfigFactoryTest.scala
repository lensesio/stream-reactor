package com.datamountaineer.streamreactor.connect.pulsar

import com.datamountaineer.streamreactor.connect.pulsar.config.{PulsarConfigConstants, PulsarSinkConfig, PulsarSinkSettings}
import org.apache.pulsar.client.api.{CompressionType, MessageRoutingMode, ProducerBuilder, PulsarClient}
import org.mockito.ArgumentMatchersSugar.any
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.MapHasAsJava


/**
  * Created by andrew@datamountaineer.com on 23/01/2018. 
  * stream-reactor
  */
class ProducerConfigFactoryTest extends AnyWordSpec with Matchers with MockitoSugar with BeforeAndAfter {

  val pulsarClient = mock[PulsarClient]
  val pulsarTopic = "persistent://landoop/standalone/connect/kafka-topic"

  val producerBuilder = mock[ProducerBuilder[Array[Byte]]]
  val producerConfigFactory = new ProducerConfigFactory(pulsarClient)

  before {
    reset(pulsarClient, producerBuilder)

    when(pulsarClient.newProducer()).thenReturn(producerBuilder)

  }

  "should create a SinglePartition with batching" in {
    val config = PulsarSinkConfig(Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic BATCH = 10 WITHPARTITIONER = SinglePartition WITHCOMPRESSION = ZLIB WITHDELAY = 1000"
    ).asJava)

    val settings = PulsarSinkSettings(config)
    val producerConfig = producerConfigFactory("test", settings.kcql)

    verify(producerConfig(pulsarTopic)).enableBatching(true)
    verify(producerConfig(pulsarTopic)).batchingMaxMessages(10)
    verify(producerConfig(pulsarTopic)).batchingMaxPublishDelay(1000, TimeUnit.MILLISECONDS)

    verify(producerConfig(pulsarTopic)).compressionType(CompressionType.ZLIB)
    verify(producerConfig(pulsarTopic)).messageRoutingMode(MessageRoutingMode.SinglePartition)
  }

  "should create a CustomPartition with no batching and no compression" in {
    val config = PulsarSinkConfig(Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic WITHPARTITIONER = CustomPartition"
    ).asJava)

    val settings = PulsarSinkSettings(config)
    val producerConfig = producerConfigFactory("test", settings.kcql)

    verify(producerConfig(pulsarTopic), never).enableBatching(true)
    verify(producerConfig(pulsarTopic), never).compressionType(any[CompressionType])
    verify(producerConfig(pulsarTopic)).messageRoutingMode(MessageRoutingMode.CustomPartition)
  }

  "should create a roundrobin with batching and no compression no delay" in {
    val config = PulsarSinkConfig(Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic BATCH  = 10 WITHPARTITIONER = ROUNDROBINPARTITION"
    ).asJava)

    val settings = PulsarSinkSettings(config)
    val producerConfig = producerConfigFactory("test", settings.kcql)

    verify(producerConfig(pulsarTopic)).enableBatching(true)
    verify(producerConfig(pulsarTopic)).batchingMaxMessages(10)

    verify(producerConfig(pulsarTopic), never).batchingMaxPublishDelay(any[Long], any[TimeUnit])
    verify(producerConfig(pulsarTopic), never).compressionType(CompressionType.NONE)
    verify(producerConfig(pulsarTopic)).messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
  }
}
