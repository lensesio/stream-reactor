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

package com.datamountaineer.streamreactor.connect.sink

import java.io.File
import java.util
import java.util.UUID

import com.datamountaineer.streamreactor.connect.jms.sink.JMSSinkTask
import com.datamountaineer.streamreactor.connect.{TestBase, Using}
import com.fasterxml.jackson.databind.node.{ArrayNode, IntNode}
import javax.jms.{Message, MessageListener, Session, TextMessage}
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.json.JsonDeserializer
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll

import scala.reflect.io.Path


class JMSSinkTaskTest extends TestBase with Using with BeforeAndAfterAll with MockitoSugar {
  val broker = new BrokerService()
  broker.setPersistent(false)
  broker.setUseJmx(false)
  broker.setDeleteAllMessagesOnStartup(true)
  val brokerUrl = JMS_URL_1
  broker.addConnector(brokerUrl)
  broker.setUseShutdownHook(false)
  val property = "java.io.tmpdir"
  val tempDir = System.getProperty(property)
  broker.setDataDirectoryFile( new File(tempDir))

  override def beforeAll {
    broker.start()
  }


  override def afterAll(): Unit = {
    broker.stop()
    Path(AVRO_FILE).delete()
  }

  "JMSSinkTask write records to JMS" in {

      val kafkaTopic1 = s"kafka1-${UUID.randomUUID().toString}"
      val kafkaTopic2 = s"kafka2-${UUID.randomUUID().toString}"
      val queueName = UUID.randomUUID().toString
      val topicName = UUID.randomUUID().toString

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

          val topic = session.createTopic(topicName)
          val topicConsumer = session.createConsumer(topic)

          val topicMsgListener = new MessageListener {
            @volatile var msg: Message = _

            override def onMessage(message: Message): Unit = {
              msg = message
            }
          }
          topicConsumer.setMessageListener(topicMsgListener)

          val queue = session.createQueue(queueName)
          val consumerQueue = session.createConsumer(queue)

          val queueMsgListener = new MessageListener {
            @volatile var msg: Message = _

            override def onMessage(message: Message): Unit = {
              msg = message
            }
          }
          consumerQueue.setMessageListener(queueMsgListener)

          val kcql = s"${getKCQL(topicName, kafkaTopic1, "TOPIC")};${getKCQL(queueName, kafkaTopic2, "QUEUE")}"
          val props = getSinkProps(kcql, kafkaTopic1, brokerUrl)
          val context = mock[SinkTaskContext]
          val topicsSet = new util.HashSet[TopicPartition]()
          topicsSet.add(new TopicPartition(kafkaTopic1, 0))
          topicsSet.add(new TopicPartition(kafkaTopic2, 0))
          when(context.assignment()).thenReturn(topicsSet)
          when(context.configs()).thenReturn(props)

          val task = new JMSSinkTask
          task.initialize(context)
          task.start(props)

          val records = new java.util.ArrayList[SinkRecord]
          records.add(record1)
          records.add(record2)
          task.put(records)

          Thread.sleep(1000)
          val queueMessage = queueMsgListener.msg.asInstanceOf[TextMessage]
          val topicMessage = topicMsgListener.msg.asInstanceOf[TextMessage]

          queueMessage != null shouldBe true
          topicMessage != null shouldBe true

          //can not do json text comparison because fields order is not guaranteed
          val deserializer = new JsonDeserializer()
          val queueJson = deserializer.deserialize("", queueMessage.getText.getBytes)
          //queueJson.get("byte").asInt() shouldBe 12
          //queueJson.get("short").asInt() shouldBe 12
          queueJson.get("int32").asInt() shouldBe 12
          queueJson.get("int64").asInt() shouldBe 12
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

          IteratorToSeqFn(queueJson.get("mapNonStringKeys").asInstanceOf[ArrayNode].iterator()).flatMap { t =>
            IteratorToSeqFn(queueJson.get("mapNonStringKeys").asInstanceOf[ArrayNode].iterator().next().asInstanceOf[ArrayNode].iterator())
              .map(_.asInt())
          }.toVector shouldBe Vector(1, 1)

          val topicJson = deserializer.deserialize("", topicMessage.getText.getBytes)
          topicJson.get("int8").asInt() shouldBe 12
          topicJson.get("int16").asInt() shouldBe 12
          //topicJson.get("int").asInt() shouldBe 12
          //topicJson.get("long").asInt() shouldBe 12
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

          IteratorToSeqFn(topicJson.get("mapNonStringKeys").asInstanceOf[ArrayNode].iterator()).flatMap { t =>
            IteratorToSeqFn(topicJson.get("mapNonStringKeys").asInstanceOf[ArrayNode].iterator().next().asInstanceOf[ArrayNode].iterator())
              .map(_.asInt())
          }.toVector shouldBe Vector(1, 1)
        }
      }
    }
}
