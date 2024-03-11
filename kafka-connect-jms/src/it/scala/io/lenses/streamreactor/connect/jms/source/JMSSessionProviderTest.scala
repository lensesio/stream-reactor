/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package io.lenses.streamreactor.connect.jms.source

import io.lenses.streamreactor.connect.fixtures.broker.testWithBroker
import io.lenses.streamreactor.connect.fixtures.broker.testWithBrokerOnPort
import io.lenses.streamreactor.connect.jms.config.DestinationSelector
import io.lenses.streamreactor.connect.jms.config.JMSConfig
import io.lenses.streamreactor.connect.jms.config.JMSSettings
import io.lenses.streamreactor.connect.jms.ItTestBase
import io.lenses.streamreactor.connect.jms.JMSSessionProvider
import org.apache.activemq.ActiveMQConnection
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

import java.util.UUID
import jakarta.jms.Session
import javax.naming.NameNotFoundException
import scala.util.Try

class JMSSessionProviderTest extends ItTestBase with BeforeAndAfterAll with Eventually {

  val forAJmsConsumer = false
  val forAJmsProducer = true

  "should only create JMS Queue Consumer when reading from JMS Queue" in testWithBrokerOnPort { (_, brokerUrl) =>
    val kafkaTopic = s"kafka-${UUID.randomUUID().toString}"
    val queueName  = UUID.randomUUID().toString
    val kcql       = getKCQL(kafkaTopic, queueName, "QUEUE")
    val props      = getProps(kcql, brokerUrl)
    val config     = JMSConfig(props)
    val settings   = JMSSettings(config, forAJmsConsumer)
    val provider   = JMSSessionProvider(settings, forAJmsConsumer)
    provider.queueConsumers.size shouldBe 1
    provider.queueProducers.size shouldBe 0
    provider.topicsConsumers.size shouldBe 0
    provider.topicProducers.size shouldBe 0
    ()
  }

  "should only create JMS Topic Consumer when reading from JMS Topic" in testWithBrokerOnPort { (_, brokerUrl) =>
    val kafkaTopic = s"kafka-${UUID.randomUUID().toString}"
    val topicName  = UUID.randomUUID().toString
    val kcql       = getKCQL(kafkaTopic, topicName, "TOPIC")
    val props      = getProps(kcql, brokerUrl)
    val config     = JMSConfig(props)
    val settings   = JMSSettings(config, forAJmsConsumer)
    val provider   = JMSSessionProvider(settings, forAJmsConsumer)
    provider.queueConsumers.size shouldBe 0
    provider.queueProducers.size shouldBe 0
    provider.topicsConsumers.size shouldBe 1
    provider.topicProducers.size shouldBe 0
    ()
  }

  "should only create JMS Queue Producer when writing to JMS Queue" in testWithBrokerOnPort { (_, brokerUrl) =>
    val kafkaTopic = s"kafka-${UUID.randomUUID().toString}"
    val queueName  = UUID.randomUUID().toString
    val kcql       = getKCQL(kafkaTopic, queueName, "QUEUE")
    val props      = getProps(kcql, brokerUrl)
    val config     = JMSConfig(props)
    val settings   = JMSSettings(config, forAJmsProducer)
    val provider   = JMSSessionProvider(settings, forAJmsProducer)
    provider.queueConsumers.size shouldBe 0
    provider.queueProducers.size shouldBe 1
    provider.topicsConsumers.size shouldBe 0
    provider.topicProducers.size shouldBe 0
    ()
  }

  "should only create JMS Topic Producer when writing to JMS Topic" in testWithBrokerOnPort { (_, brokerUrl) =>
    val kafkaTopic = s"kafka-${UUID.randomUUID().toString}"
    val topicName  = UUID.randomUUID().toString
    val kcql       = getKCQL(kafkaTopic, topicName, "TOPIC")
    val props      = getProps(kcql, brokerUrl)
    val config     = JMSConfig(props)
    val settings   = JMSSettings(config, forAJmsProducer)
    val provider   = JMSSessionProvider(settings, forAJmsProducer)
    provider.queueConsumers.size shouldBe 0
    provider.queueProducers.size shouldBe 0
    provider.topicsConsumers.size shouldBe 0
    provider.topicProducers.size shouldBe 1
    ()
  }

  "should close the connection when the task is stopped" in testWithBrokerOnPort { (_, brokerUrl) =>
    val kafkaTopic = s"kafka-${UUID.randomUUID().toString}"
    val topicName  = UUID.randomUUID().toString
    val kcql       = getKCQL(kafkaTopic, topicName, "TOPIC")
    val props      = getProps(kcql, brokerUrl)
    val config     = JMSConfig(props)
    val settings   = JMSSettings(config, forAJmsProducer)
    val provider   = JMSSessionProvider(settings, forAJmsProducer)
    provider.close().isSuccess shouldBe true
    Try(provider.connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)).isFailure shouldBe true
    ()
  }

  "should close connection and free resources on exception when configuring session provider" in
    testWithBroker(clientID = Some("static-client-id")) { brokerUrl =>
      val kafkaTopic      = s"kafka-${UUID.randomUUID().toString}"
      val topicName       = UUID.randomUUID().toString
      val kcql            = getKCQL(kafkaTopic, topicName, "TOPIC")
      val props           = getProps(kcql, brokerUrl)
      val config          = JMSConfig(props)
      val validSettings   = JMSSettings(config, forAJmsConsumer)
      val invalidSettings = validSettings.copy(destinationSelector = DestinationSelector.JNDI)

      assertThrows[NameNotFoundException] {
        JMSSessionProvider(invalidSettings, forAJmsConsumer)
      }
      val provider = JMSSessionProvider(validSettings, forAJmsConsumer)
      provider.connection.asInstanceOf[ActiveMQConnection].isClosed shouldBe false
      provider.connection.close()
    }
}
