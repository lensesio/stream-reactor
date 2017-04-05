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
import com.datamountaineer.streamreactor.connect.jms.source.JMSSourceTask
import com.datamountaineer.streamreactor.connect.jms.source.domain.JMSStructMessage
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 24/03/2017. 
  * stream-reactor
  */
class JMSSourceTaskTest extends TestBase {
  "should start a JMSSourceTask and read records" in {
    val broker = new BrokerService()
    broker.setPersistent(false)
    broker.setUseJmx(false)
    broker.setDeleteAllMessagesOnStartup(true)
    val brokerUrl = "tcp://localhost:61640"
    broker.addConnector(brokerUrl)
    broker.setUseShutdownHook(false)
    val property = "java.io.tmpdir"
    val tempDir = System.getProperty(property)
    broker.setTmpDataDirectory( new File(tempDir))
    broker.start()

    val props = getPropsMixCDI(brokerUrl)
    val task = new JMSSourceTask()
    task.start(props)

    //send in some records to the JMS queue
    //KCQL_SOURCE_QUEUE

    val connectionFactory = new ActiveMQConnectionFactory()
    connectionFactory.setBrokerURL(brokerUrl)
    val conn = connectionFactory.createConnection()
    conn.start()
    val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val queue = session.createQueue(QUEUE1)
    val queueProducer = session.createProducer(queue)
    val messages = getTextMessages(10, session)
    messages.foreach(m => queueProducer.send(m))

    val records = task.poll().asScala
    records.size shouldBe 10

    records.head.valueSchema().toString shouldBe JMSStructMessage.getSchema().toString
    task.stop()
  }
}
