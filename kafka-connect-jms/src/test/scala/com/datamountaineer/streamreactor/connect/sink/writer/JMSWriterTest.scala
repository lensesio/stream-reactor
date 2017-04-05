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

package com.datamountaineer.streamreactor.connect.sink.writer

import java.io.File
import javax.jms.{Message, MessageListener, Session, TextMessage}

import com.datamountaineer.streamreactor.connect.TestBase
import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSSettings}
import com.datamountaineer.streamreactor.connect.jms.sink.writer.JMSWriter
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.datamountaineer.streamreactor.connect.sink.IteratorToSeqFn
import com.fasterxml.jackson.databind.node.{ArrayNode, IntNode}
import com.sksamuel.scalax.io.Using
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService
import org.apache.kafka.connect.json.JsonDeserializer
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.BeforeAndAfter

class JMSWriterTest extends TestBase with Using with BeforeAndAfter with ConverterUtil {
  val broker = new BrokerService()
  broker.setPersistent(false)
  broker.setUseJmx(false)
  broker.setDeleteAllMessagesOnStartup(true)
  val brokerUrl = "tcp://localhost:61620"
  broker.addConnector(brokerUrl)
  broker.setUseShutdownHook(false)
  val property = "java.io.tmpdir"
  val tempDir = System.getProperty(property)
  broker.setTmpDataDirectory( new File(tempDir))

  before {
    broker.start()
  }

  after {
    broker.stop()
  }

  "JMSWriter" should {
    "route the messages to the appropriate topic and queues" in {
      val kafkaTopic1 = TOPIC1
      val kafkaTopic2 = TOPIC2

      val schema = getSchema
      val struct = getStruct(schema)

      val record1 = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)
      val record2 = new SinkRecord(kafkaTopic2, 0, null, null, schema, struct, 5)

      val connectionFactory = new ActiveMQConnectionFactory()
      connectionFactory.setBrokerURL(brokerUrl)
      using(connectionFactory.createConnection()) { connection =>
        connection.setClientID("bibble")
        connection.start()

        using(connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) { session =>

          val topic = session.createTopic(TOPIC1)
          val topicConsumer = session.createConsumer(topic)

          val topicMsgListener = new MessageListener {
            @volatile var msg: Message = _

            override def onMessage(message: Message): Unit = {
              msg = message
            }
          }
          topicConsumer.setMessageListener(topicMsgListener)

          val queue = session.createQueue(QUEUE1)
          val consumerQueue = session.createConsumer(queue)

          val queueMsgListener = new MessageListener {
            @volatile var msg: Message = _

            override def onMessage(message: Message): Unit = {
              msg = message
            }
          }
          consumerQueue.setMessageListener(queueMsgListener)

          val props = getPropsMixJNDIWithSink()
          val config = JMSConfig(props)
          val settings = JMSSettings(config, true)
          val writer = JMSWriter(settings)
          writer.write(Seq(record1, record2))

          Thread.sleep(1000)
          val queueMessage = queueMsgListener.msg.asInstanceOf[TextMessage]
          val topicMessage = topicMsgListener.msg.asInstanceOf[TextMessage]

          queueMessage != null shouldBe true
          topicMessage != null shouldBe true

          //can not do json text comparison because fields order is not guaranteed
          val deserializer = new JsonDeserializer()
          val queueJson = deserializer.deserialize("", queueMessage.getText.getBytes)
          queueJson.get("int8").asInt() shouldBe 12
          queueJson.get("int16").asInt() shouldBe 12
          //queueJson.get("long").asInt() shouldBe 12
          queueJson.get("float32").asDouble() shouldBe 12.2
          queueJson.get("float64").asDouble() shouldBe 12.2
          queueJson.get("boolean").asBoolean() shouldBe true
          queueJson.get("string").asText() shouldBe "foo"
          queueJson.get("bytes").asText() shouldBe "Zm9v"
          IteratorToSeqFn(queueJson.get("array").asInstanceOf[ArrayNode].iterator()).map {
            _.asText()
          }.toSet shouldBe Set("a", "b", "c")
          IteratorToSeqFn(queueJson.get("map").fields()).map { t =>
            t.getKey -> t.getValue.asInstanceOf[IntNode].asInt()
          }.toMap shouldBe Map("field" -> 1)

          IteratorToSeqFn(queueJson.get("mapNonStringKeys").asInstanceOf[ArrayNode].iterator()).flatMap { _ =>
            IteratorToSeqFn(queueJson.get("mapNonStringKeys").asInstanceOf[ArrayNode].iterator().next().asInstanceOf[ArrayNode].iterator())
              .map(_.asInt())
          }.toVector shouldBe Vector(1, 1)

          val topicJson = deserializer.deserialize("", topicMessage.getText.getBytes)
         // topicJson.get("byte").asInt() shouldBe 12
         // topicJson.get("short").asInt() shouldBe 12
          topicJson.get("int32").asInt() shouldBe 12
          topicJson.get("int64").asInt() shouldBe 12
          topicJson.get("float32").asDouble() shouldBe 12.2
          topicJson.get("float64").asDouble() shouldBe 12.2
          topicJson.get("boolean").asBoolean() shouldBe true
          topicJson.get("string").asText() shouldBe "foo"
          topicJson.get("bytes").asText() shouldBe "Zm9v"
          IteratorToSeqFn(topicJson.get("array").asInstanceOf[ArrayNode].iterator()).map {
            _.asText()
          }.toSet shouldBe Set("a", "b", "c")

          IteratorToSeqFn(topicJson.get("map").fields()).map { t =>
            t.getKey -> t.getValue.asInstanceOf[IntNode].asInt()
          }.toMap shouldBe Map("field" -> 1)

          IteratorToSeqFn(topicJson.get("mapNonStringKeys").asInstanceOf[ArrayNode].iterator()).flatMap { _ =>
            IteratorToSeqFn(topicJson.get("mapNonStringKeys").asInstanceOf[ArrayNode].iterator().next().asInstanceOf[ArrayNode].iterator())
              .map(_.asInt())
          }.toVector shouldBe Vector(1, 1)
        }
      }

    }
  }
}


