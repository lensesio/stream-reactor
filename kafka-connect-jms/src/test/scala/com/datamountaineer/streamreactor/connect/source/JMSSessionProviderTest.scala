package com.datamountaineer.streamreactor.connect.source

import com.datamountaineer.streamreactor.connect.TestBase
import com.datamountaineer.streamreactor.connect.jms.JMSSessionProvider
import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSSettings}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

class JMSSessionProviderTest extends TestBase with BeforeAndAfterAll with Eventually {

  val forAJmsConsumer = false
  val forAJmsProducer = true

  "should only create JMS Queue Consumer when reading from JMS Queue" in testWithBrokerOnPort { (conn, brokerUrl) =>
    val props = getProps1Queue(brokerUrl)
    val config = JMSConfig(props)
    val settings = JMSSettings(config, forAJmsConsumer)
    val provider = JMSSessionProvider(settings, forAJmsConsumer)

    provider.queueConsumers.size shouldBe 1
    provider.queueProducers.size shouldBe 0
    provider.topicsConsumers.size shouldBe 0
    provider.topicProducers.size shouldBe 0
  }

  "should only create JMS Topic Consumer when reading from JMS Topic" in testWithBrokerOnPort { (conn, brokerUrl) =>
    val props = getProps1Topic(brokerUrl)
    val config = JMSConfig(props)
    val settings = JMSSettings(config, forAJmsConsumer)
    val provider = JMSSessionProvider(settings, forAJmsConsumer)

    provider.queueConsumers.size shouldBe 0
    provider.queueProducers.size shouldBe 0
    provider.topicsConsumers.size shouldBe 1
    provider.topicProducers.size shouldBe 0
  }

  "should only create JMS Queue Producer when writing to JMS Queue" in testWithBrokerOnPort { (conn, brokerUrl) =>
    val props = getProps1Queue(brokerUrl)
    val config = JMSConfig(props)
    val settings = JMSSettings(config, forAJmsProducer)
    val provider = JMSSessionProvider(settings, forAJmsProducer)

    provider.queueConsumers.size shouldBe 0
    provider.queueProducers.size shouldBe 1
    provider.topicsConsumers.size shouldBe 0
    provider.topicProducers.size shouldBe 0
  }

  "should only create JMS Topic Producer when writing to JMS Topic" in testWithBrokerOnPort { (conn, brokerUrl) =>
    val props = getProps1Topic(brokerUrl)
    val config = JMSConfig(props)
    val settings = JMSSettings(config, forAJmsProducer)
    val provider = JMSSessionProvider(settings, forAJmsProducer)

    provider.queueConsumers.size shouldBe 0
    provider.queueProducers.size shouldBe 0
    provider.topicsConsumers.size shouldBe 0
    provider.topicProducers.size shouldBe 1
  }
}
