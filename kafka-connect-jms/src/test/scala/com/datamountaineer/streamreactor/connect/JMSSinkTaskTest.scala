package com.datamountaineer.streamreactor.connect

import java.util
import javax.jms.{Message, MessageListener, Session, TextMessage}

import com.datamountaineer.streamreactor.connect.hbase.JMSSinkTask
import com.datamountaineer.streamreactor.connect.jms.sink.config._
import com.fasterxml.jackson.databind.node.{ArrayNode, IntNode}
import com.sksamuel.scalax.io.Using
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.json.JsonDeserializer
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._

class JMSSinkTaskTest extends WordSpec with Matchers with Using with BeforeAndAfter with MockitoSugar {
  val broker = new BrokerService()
  broker.setPersistent(false)
  broker.setUseJmx(false)
  broker.setDeleteAllMessagesOnStartup(true)
  val brokerUrl = "tcp://localhost:61621"
  broker.addConnector(brokerUrl)
  broker.setUseShutdownHook(false)

  before {
    broker.start()
  }

  after {
    broker.stop()
  }

  "JMSSinkTask" should {
    "redirect the connect records to JMS" in {

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
            @volatile var msg: Message = null

            override def onMessage(message: Message): Unit = {
              msg = message
            }
          }
          topicConsumer.setMessageListener(topicMsgListener)

          val queue = session.createQueue(jmsQueue)
          val consumerQueue = session.createConsumer(queue)

          val queueMsgListener = new MessageListener {
            @volatile var msg: Message = null

            override def onMessage(message: Message): Unit = {
              msg = message
            }
          }
          consumerQueue.setMessageListener(queueMsgListener)


          val context = mock[SinkTaskContext]
          val topicsSet = new util.HashSet[TopicPartition]()
          topicsSet.add(new TopicPartition(kafkaTopic1, 0))
          topicsSet.add(new TopicPartition(kafkaTopic2, 0))
          when(context.assignment()).thenReturn(topicsSet)

          val task = new JMSSinkTask
          task.initialize(context)
          task.start(
            Map(
              JMSSinkConfig.CONNECTION_FACTORY -> classOf[ActiveMQConnectionFactory].getCanonicalName,
              JMSSinkConfig.JMS_URL -> brokerUrl,
              JMSSinkConfig.MESSAGE_TYPE -> "JSON",
              JMSSinkConfig.EXPORT_ROUTE_QUERY ->
                s"""
                   |INSERT INTO $jmsQueue SELECT *,int8 as byte, int16 as short from $kafkaTopic1;
                   |INSERT INTO $jmsTopic SELECT *,int32 as int, int64 as long FROM $kafkaTopic2""".stripMargin,
              JMSSinkConfig.TOPICS_LIST -> s"$jmsTopic",
              JMSSinkConfig.QUEUES_LIST -> s"$jmsQueue"
            ).asJava
          )


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
          queueJson.get("byte").asInt() shouldBe 12
          queueJson.get("short").asInt() shouldBe 12
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
          topicJson.get("int").asInt() shouldBe 12
          topicJson.get("long").asInt() shouldBe 12
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
}
