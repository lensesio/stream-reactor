/*
 *  Copyright 2016 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.writer

import javax.jms.{Message, MessageListener, Session, TextMessage}

import com.datamountaineer.streamreactor.connect.IteratorToSeqFn
import com.datamountaineer.streamreactor.connect.jms.sink.config._
import com.datamountaineer.streamreactor.connect.jms.sink.writer.JMSWriter
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.fasterxml.jackson.databind.node.{ArrayNode, IntNode}
import com.sksamuel.scalax.io.Using
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.json.JsonDeserializer
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._

class JMSWriterTest extends WordSpec with Matchers with Using with BeforeAndAfter with ConverterUtil {
  val broker = new BrokerService()
  broker.setPersistent(false)
  broker.setUseJmx(false)
  broker.setDeleteAllMessagesOnStartup(true)
  val brokerUrl = "tcp://localhost:61620"
  broker.addConnector(brokerUrl)
  broker.setUseShutdownHook(false)

  before {
    broker.start()
  }

  after {
    broker.stop()
  }

  "JMSWriter" should {
    "route the messages to the appropriate topic and queues" in {
      val kafkaTopic1 = "kafkaTopic1"
      val kafkaTopic2 = "kafkaTopic2"

      val jmsTopic = "topic1"
      val jmsQueue = "queue1"

      val builder = SchemaBuilder.struct
        .field("int8", SchemaBuilder.int8().defaultValue(2.toByte).doc("int8 field").build())
        .field("int16", Schema.INT16_SCHEMA)
        .field("int32", Schema.INT32_SCHEMA)
        .field("int64", Schema.INT64_SCHEMA)
        .field("float32", Schema.FLOAT32_SCHEMA)
        .field("float64", Schema.FLOAT64_SCHEMA)
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .field("bytes", Schema.BYTES_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
        .field("mapNonStringKeys", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA)
          .build())
      val schema = builder.build()
      val struct = new Struct(schema)
        .put("int8", 12.toByte)
        .put("int16", 12.toShort)
        .put("int32", 12)
        .put("int64", 12L)
        .put("float32", 12.2f)
        .put("float64", 12.2)
        .put("boolean", true)
        .put("string", "foo")
        .put("bytes", "foo".getBytes())
        .put("array", List("a", "b", "c").asJava)
        .put("map", Map("field" -> 1).asJava)
        .put("mapNonStringKeys", Map(1 -> 1).asJava)

      val record1 = new SinkRecord(kafkaTopic1, 0, null, null, schema, struct, 1)
      val record2 = new SinkRecord(kafkaTopic2, 0, null, null, schema, struct, 5)

      val connectionFactory = new ActiveMQConnectionFactory()
      connectionFactory.setBrokerURL(brokerUrl)
      using(connectionFactory.createConnection()) { connection =>
        connection.setClientID("bibble")
        connection.start()

        using(connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) { session =>

          val topic = session.createTopic(jmsTopic)
          val topicConsumer = session.createConsumer(topic)

          val topicMsgListener = new MessageListener {
            @volatile var msg: Message = _

            override def onMessage(message: Message): Unit = {
              msg = message
            }
          }
          topicConsumer.setMessageListener(topicMsgListener)

          val queue = session.createQueue(jmsQueue)
          val consumerQueue = session.createConsumer(queue)

          val queueMsgListener = new MessageListener {
            @volatile var msg: Message = _

            override def onMessage(message: Message): Unit = {
              msg = message
            }
          }
          consumerQueue.setMessageListener(queueMsgListener)

          val writer =
            JMSWriter(
              JMSSettings(
                brokerUrl,
                classOf[ActiveMQConnectionFactory],
                List(
                  JMSConfig(TopicDestination, kafkaTopic1, jmsTopic, includeAllFields = true, Map("int8" -> "byte", "int16" -> "short")),
                  JMSConfig(QueueDestination, kafkaTopic2, jmsQueue, includeAllFields = true, Map("int32" -> "int", "int64" -> "long"))),
                None,
                None,
                retries = 1))
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
          queueJson.get("long").asInt() shouldBe 12
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
          topicJson.get("byte").asInt() shouldBe 12
          topicJson.get("short").asInt() shouldBe 12
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


