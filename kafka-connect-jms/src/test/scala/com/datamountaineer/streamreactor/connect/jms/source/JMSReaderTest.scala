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

package com.datamountaineer.streamreactor.connect.jms.source

import com.datamountaineer.streamreactor.connect.converters.source.AvroConverter
import com.datamountaineer.streamreactor.connect.jms.TestBase
import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSSettings}
import com.datamountaineer.streamreactor.connect.jms.source.domain.JMSStructMessage
import com.datamountaineer.streamreactor.connect.jms.source.readers.JMSReader
import org.apache.kafka.connect.data.Struct
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

import java.nio.charset.StandardCharsets
import java.util.UUID
import javax.jms.Session
import scala.collection.JavaConverters._
import scala.reflect.io.Path

/**
 * Created by andrew@datamountaineer.com on 20/03/2017.
 * stream-reactor
 */
class JMSReaderTest extends TestBase with BeforeAndAfterAll with Eventually {

  override def afterAll(): Unit = {
    Path(AVRO_FILE).delete()
  }

  "should read message from JMS queue without converters" in testWithBrokerOnPort { (conn, brokerUrl) =>

    val messageCount = 9
    val queueName = s"no-converters-${UUID.randomUUID().toString}"
    val kafkaTopic = s"kafka-${UUID.randomUUID().toString}"

    val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val queue = session.createQueue(queueName)
    val queueProducer = session.createProducer(queue)
    val messages = getTextMessages(messageCount, session)
    messages.foreach(m => queueProducer.send(m))

    val kcql = getKCQL(kafkaTopic, queueName, "QUEUE")
    val props = getProps(kcql, brokerUrl)

    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, false)
    val reader = JMSReader(settings)

    eventually {
      val messagesRead = reader.poll()
      messagesRead.size shouldBe messageCount
      messagesRead.head._2.valueSchema().toString shouldBe JMSStructMessage.getSchema().toString
    }
  }

  "should read and convert to avro" in testWithBrokerOnPort { (conn, brokerUrl) =>

    val messageCount = 10
    val kafkaTopic = s"kafka-${UUID.randomUUID().toString}"
    val queueName = s"avro-${UUID.randomUUID().toString}"

    val kcql = getKCQLAvroSource(kafkaTopic, queueName, "QUEUE")
    val props = getProps(kcql, brokerUrl) ++ Map(AvroConverter.CONNECT_SOURCE_CONVERTER_SCHEMA_CONFIG -> getAvroProp(queueName))

    val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val avro = session.createQueue(queueName)
    val avroProducer = session.createProducer(avro)
    val avroMessages = getBytesMessage(messageCount, session)
    avroMessages.foreach(m => avroProducer.send(m))

    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, false)
    val reader = JMSReader(settings)

    eventually {
      val messagesRead = reader.poll().toVector
      messagesRead.nonEmpty shouldBe true
      val sourceRecord = messagesRead.head._2
      sourceRecord.value().isInstanceOf[Struct] shouldBe true
      val struct = sourceRecord.value().asInstanceOf[Struct]
      val d=struct.getBytes("bytes_payload")
      val str = new String(d, StandardCharsets.UTF_8)
      str.contains("andrew") shouldBe true
    }
  }

  "should read messages from JMS queue with message selector" in testWithBrokerOnPort { (conn, brokerUrl) =>
    val messageCount = 10

    val kafkaTopic = s"kafka-${UUID.randomUUID().toString}"
    val topicName = s"selector-${UUID.randomUUID().toString}"

    val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val topic = session.createTopic(topicName)
    val topicProducer = session.createProducer(topic)

    val messages = getTextMessages(messageCount / 2, session).map { m =>
      m.setStringProperty("Fruit", "apples")
      m
    } ++ getTextMessages(messageCount / 2, session).map { m =>
      m.setStringProperty("Fruit", "pears")
      m
    }

    val messageSelector = "Fruit='apples'"
    val kcql = kcqlWithMessageSelector(kafkaTopic, topicName, messageSelector)
    val props = getProps(kcql, brokerUrl)
    val config = JMSConfig(props.asJava)
    val settings = JMSSettings(config, false)
    val reader = JMSReader(settings)

    messages.foreach(m => topicProducer.send(m))

    Thread.sleep(2000)

    val messagesRead = reader.poll()
    messagesRead.size shouldBe messageCount / 2
    messagesRead.foreach { case (msg, _) =>
      msg.getStringProperty("Fruit") shouldBe "apples"
    }

    val sourceRecord = messagesRead.head._2
    sourceRecord.valueSchema().toString shouldBe JMSStructMessage.getSchema().toString
    sourceRecord.value().isInstanceOf[Struct] shouldBe true

    val struct = sourceRecord.value().asInstanceOf[Struct]
    struct.getMap("properties").asScala shouldBe Map("Fruit" -> "apples")
  }
}
