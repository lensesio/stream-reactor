/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.source

import java.io.File
import javax.jms.{Connection, Session}

import com.datamountaineer.streamreactor.connect.TestBase
import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSSettings}
import com.datamountaineer.streamreactor.connect.jms.source.domain.JMSStructMessage
import com.datamountaineer.streamreactor.connect.jms.source.readers.JMSReader
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService
import org.apache.kafka.connect.data.Struct
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

import scala.reflect.io.Path

/**
  * Created by andrew@datamountaineer.com on 20/03/2017. 
  * stream-reactor
  */
class JMSReaderTest extends TestBase with BeforeAndAfterAll with Eventually {

  override def afterAll(): Unit = {
    Path(AVRO_FILE).delete()
  }

  "should read message from JMS queue without converters" in testWithBrokerOnPort(61631) { (conn, brokerUrl) =>

    val messageCount = 9

    val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val queue = session.createQueue(QUEUE1)
    val queueProducer = session.createProducer(queue)
    val messages = getTextMessages(messageCount, session)
    messages.foreach(m => queueProducer.send(m))

    val props = getProps1Queue(brokerUrl)
    val config = JMSConfig(props)
    val settings = JMSSettings(config, false)
    val reader = JMSReader(settings)

    eventually {
      val messagesRead = reader.poll()
      messagesRead.size shouldBe messageCount
      messagesRead.head._2.valueSchema().toString shouldBe JMSStructMessage.getSchema().toString
    }
  }

  "should read and convert to avro" in testWithBrokerOnPort(61632) { (conn, brokerUrl) =>

    val messageCount = 10

    val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val avro = session.createQueue(AVRO_QUEUE)
    val avroProducer = session.createProducer(avro)
    val avroMessages = getBytesMessage(messageCount, session)
    avroMessages.foreach(m => avroProducer.send(m))

    val props = getPropsMixCDIWithConverters(brokerUrl)
    val config = JMSConfig(props)
    val settings = JMSSettings(config, false)
    val reader = JMSReader(settings)

    eventually {
      val messagesRead = reader.poll().toVector
      messagesRead.nonEmpty shouldBe true
      val sourceRecord = messagesRead.head._2
      sourceRecord.value().isInstanceOf[Struct] shouldBe true
      val struct = sourceRecord.value().asInstanceOf[Struct]
      struct.getString("name") shouldBe "andrew"
    }
  }

  "should read messages from JMS queue with message selector" in testWithBrokerOnPort(61633) { (conn, brokerUrl) =>
    val messageCount = 10

    val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val topic = session.createTopic(TOPIC1)
    val topicProducer = session.createProducer(topic)

    val messages = getTextMessages(messageCount / 2, session).map { m =>
      m.setStringProperty("Fruit", "apples")
      m
    } ++ getTextMessages(messageCount / 2, session).map { m =>
      m.setStringProperty("Fruit", "pears")
      m
    }

    val messageSelector = "Fruit='apples'"

    val props = getProps1TopicWithMessageSelector(brokerUrl, messageSelector)
    val config = JMSConfig(props)
    val settings = JMSSettings(config, false)
    val reader = JMSReader(settings)

    messages.foreach(m => topicProducer.send(m))

    eventually {
      val messagesRead = reader.poll()
      messagesRead.size shouldBe messageCount / 2
      messagesRead.keySet.foreach { msg =>
        msg.getStringProperty("Fruit") shouldBe "apples"
      }
      messagesRead.head._2.valueSchema().toString shouldBe JMSStructMessage.getSchema().toString
    }
  }

  def testWithBrokerOnPort(port: Int)(test: (Connection, String) => Unit) = {
    val broker = new BrokerService()
    broker.setPersistent(false)
    broker.setUseJmx(false)
    broker.setDeleteAllMessagesOnStartup(true)
    val brokerUrl = s"tcp://localhost:$port"
    broker.addConnector(brokerUrl)
    broker.setUseShutdownHook(false)
    val property = "java.io.tmpdir"
    val tempDir = System.getProperty(property)
    broker.setDataDirectoryFile( new File(tempDir))
    broker.setTmpDataDirectory( new File(tempDir))
    broker.start()

    val connectionFactory = new ActiveMQConnectionFactory()
    connectionFactory.setBrokerURL(brokerUrl)
    val conn = connectionFactory.createConnection()
    conn.start()

    test(conn, brokerUrl)
  }
}
