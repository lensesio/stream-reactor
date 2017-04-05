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
import javax.jms.Session

import com.datamountaineer.streamreactor.connect.TestBase
import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSSettings}
import com.datamountaineer.streamreactor.connect.jms.source.domain.JMSStructMessage
import com.datamountaineer.streamreactor.connect.jms.source.readers.JMSReader
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService
import org.apache.kafka.connect.data.Struct
import org.scalatest.BeforeAndAfter

/**
  * Created by andrew@datamountaineer.com on 20/03/2017. 
  * stream-reactor
  */
class JMSReaderTest extends TestBase with BeforeAndAfter {
  "should read message from JMS queue without converters" in {
    val broker = new BrokerService()
    broker.setPersistent(false)
    broker.setUseJmx(false)
    broker.setDeleteAllMessagesOnStartup(true)
    val brokerUrl = "tcp://localhost:61630"
    broker.addConnector(brokerUrl)
    broker.setUseShutdownHook(false)
    val property = "java.io.tmpdir"
    val tempDir = System.getProperty(property)
    broker.setDataDirectoryFile( new File(tempDir))
    broker.setTmpDataDirectory( new File(tempDir))
    val messageCount = 10

    broker.start()
    val connectionFactory = new ActiveMQConnectionFactory()
    connectionFactory.setBrokerURL(brokerUrl)
    val conn = connectionFactory.createConnection()
    conn.start()
    val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val queue = session.createQueue(QUEUE1)
    val queueProducer = session.createProducer(queue)
    val messages = getTextMessages(messageCount, session)
    messages.foreach(m => queueProducer.send(m))

    val props = getProps1Queue(brokerUrl)
    val config = JMSConfig(props)
    val settings = JMSSettings(config, false)
    val reader = JMSReader(settings)

    val messagesRead = reader.poll()
    messagesRead.size shouldBe messageCount
    messagesRead.head._2.valueSchema().toString shouldBe JMSStructMessage.getSchema().toString
  }

  "should read and convert to avro" in {

    val broker = new BrokerService()
    broker.setPersistent(false)
    broker.setUseJmx(false)
    broker.setDeleteAllMessagesOnStartup(true)
    val brokerUrl = "tcp://localhost:61631"
    broker.addConnector(brokerUrl)
    broker.setUseShutdownHook(false)
    val property = "java.io.tmpdir"
    val tempDir = System.getProperty(property)
    broker.setDataDirectoryFile( new File(tempDir))
    broker.setTmpDataDirectory( new File(tempDir))
    val messageCount = 10

    broker.start()
    val connectionFactory = new ActiveMQConnectionFactory()
    connectionFactory.setBrokerURL(brokerUrl)
    val conn = connectionFactory.createConnection()
    conn.start()
    val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val avro = session.createQueue(AVRO_QUEUE)
    val avroProducer = session.createProducer(avro)
    val avroMessages = getBytesMessage(messageCount, session)
    avroMessages.foreach(m => avroProducer.send(m))

    val props = getPropsMixCDIWithConverters(brokerUrl)
    val config = JMSConfig(props)
    val settings = JMSSettings(config, false)
    val reader = JMSReader(settings)

    val messagesRead = reader.poll()
    val sourceRecord = messagesRead.head._2

    sourceRecord.value().isInstanceOf[Struct] shouldBe true
    val struct = sourceRecord.value().asInstanceOf[Struct]
    struct.getString("name") shouldBe "andrew"
  }
}
