package com.datamountaineer.streamreactor.connect.source

import javax.naming.NameNotFoundException

import javax.jms.Session

import com.datamountaineer.streamreactor.connect.TestBase
import com.datamountaineer.streamreactor.connect.jms.JMSSessionProvider
import com.datamountaineer.streamreactor.connect.jms.config.{DestinationSelector, JMSConfig, JMSSettings}
import org.apache.activemq.ActiveMQConnection
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

import scala.util.Try

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

  "should close the connection when the task is stopped" in testWithBrokerOnPort { (conn, brokerUrl) =>
    val props = getProps1Topic(brokerUrl)
    val config = JMSConfig(props)
    val settings = JMSSettings(config, forAJmsProducer)
    val provider = JMSSessionProvider(settings, forAJmsProducer)

    provider.close().isSuccess shouldBe true
    Try(provider.connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)).isFailure shouldBe true
  }

  "should close connection and free resources on exception when configuring session provider" in
    testWithBroker(clientID = Some("static-client-id")) { brokerUrl =>
      val props = getProps1Topic(brokerUrl)
      val config = JMSConfig(props)
      val validSettings = JMSSettings(config, forAJmsConsumer)
      val invalidSettings = validSettings.copy(destinationSelector = DestinationSelector.JNDI)

      assertThrows[NameNotFoundException] {
        JMSSessionProvider(invalidSettings, forAJmsConsumer)
      }
      val provider = JMSSessionProvider(validSettings, forAJmsConsumer)
      provider.connection.asInstanceOf[ActiveMQConnection].isClosed shouldBe false
      provider.connection.close()
    }
}
