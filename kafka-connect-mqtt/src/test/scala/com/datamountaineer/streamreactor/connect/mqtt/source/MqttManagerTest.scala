package com.datamountaineer.streamreactor.connect.mqtt.source

import java.nio.ByteBuffer
import java.util

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.mqtt.config.MqttSourceSettings
import com.datamountaineer.streamreactor.connect.mqtt.source.converters.BytesConverter
import io.moquette.proto.messages.{AbstractMessage, PublishMessage}
import io.moquette.server.Server
import io.moquette.server.config.ClasspathConfig
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

class MqttManagerTest extends WordSpec with Matchers with BeforeAndAfter {

  val classPathConfig = new ClasspathConfig()

  val connection = "tcp://0.0.0.0:1883"
  val clientId = "MqttManagerTest"
  val qs = 1
  val connectionTimeout = 1000
  val keepAlive = 1000

  var mqttBroker: Option[Server] = None
  before {
    mqttBroker = Some(new Server())
    mqttBroker.foreach(_.startServer(classPathConfig))
  }

  after {
    mqttBroker.foreach {
      _.stopServer()
    }
  }

  "MqttManager" should {
    "process the messages on topic A and create source records with Bytes schema" in {
      val source = "/mqttSourceTopic"
      val target = "kafkaTopic"
      val sourcesToConvMap = Map(source -> new BytesConverter)
      implicit val settings = MqttSourceSettings(
        connection,
        None,
        None,
        clientId,
        sourcesToConvMap.map { case (k, v) => k -> v.getClass.getCanonicalName },
        true,
        Array(Config.parse(s"INSERT INTO $target SELECT * FROM \\$source")),
        qs,
        connectionTimeout,
        true,
        keepAlive,
        None,
        None,
        None
      )
      val mqttManager = new MqttManager(MqttClientConnectionFn.apply,
        sourcesToConvMap,
        1,
        Array(Config.parse(s"INSERT INTO $target SELECT * FROM \\$source")),
        true)

      val messages = Seq("message1", "message2")
      messages.foreach { m =>
        publishMessage(source, m.getBytes())
        Thread.sleep(50)
      }


      Thread.sleep(500)

      var records = new util.LinkedList[SourceRecord]()
      mqttManager.getRecords(records)

      records.size() shouldBe 2
      records.get(0).topic() shouldBe target
      records.get(1).topic() shouldBe target

      records.get(0).value() shouldBe messages(0).getBytes()
      records.get(1).value() shouldBe messages(1).getBytes()


      records.get(0).valueSchema() shouldBe Schema.BYTES_SCHEMA
      records.get(1).valueSchema() shouldBe Schema.BYTES_SCHEMA

      val msg3 = "message3".getBytes
      publishMessage(source, msg3)

      Thread.sleep(500)

      records = new util.LinkedList[SourceRecord]()
      mqttManager.getRecords(records)

      records.size() shouldBe 1
      records.get(0).topic() shouldBe target
      records.get(0).value() shouldBe msg3
      records.get(0).valueSchema() shouldBe Schema.BYTES_SCHEMA

      mqttManager.close()
    }
  }

  private def publishMessage(topic: String, payload: Array[Byte]) = {
    val message = new PublishMessage()
    message.setTopicName(s"$topic")
    message.setRetainFlag(false)
    message.setQos(AbstractMessage.QOSType.EXACTLY_ONCE)
    message.setPayload(ByteBuffer.wrap(payload))
    mqttBroker.foreach(_.internalPublish(message))

  }

}
