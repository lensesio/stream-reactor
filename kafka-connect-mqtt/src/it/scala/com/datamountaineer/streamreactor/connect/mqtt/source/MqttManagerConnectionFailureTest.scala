package com.datamountaineer.streamreactor.connect.mqtt.source

import com.datamountaineer.streamreactor.connect.converters.source.BytesConverter
import com.datamountaineer.streamreactor.connect.mqtt.SlowTest
import com.datamountaineer.streamreactor.connect.mqtt.config.MqttSourceSettings
import com.datamountaineer.streamreactor.connect.mqtt.connection.MqttClientConnectionFn
import com.dimafeng.testcontainers.ForAllTestContainer
import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.MultipleContainers
import com.dimafeng.testcontainers.ToxiproxyContainer
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.testcontainers.containers.Network

import java.util
import java.util.UUID

class MqttManagerConnectionFailureTest extends AnyWordSpec with ForAllTestContainer with Matchers {

  val mqttPort = 1883
  val cNetwork = Network.newNetwork

  val mqttContainer = GenericContainer("eclipse-mosquitto:1.4.12", exposedPorts = Seq(mqttPort))
  mqttContainer.container.setNetwork(cNetwork)

  val toxiProxyContainer = ToxiproxyContainer()
  toxiProxyContainer.container.setNetwork(cNetwork)

  override val container: MultipleContainers = MultipleContainers(mqttContainer, toxiProxyContainer)

  "MqttManager" should {

    "resubscribe after losing the connection with the broker" taggedAs SlowTest in {

      // mqtt broker port will be mapped to a different host network port upon restart
      // using a proxy container to overcome this
      val proxy = toxiProxyContainer.container.getProxy(mqttContainer.container, mqttPort)

      val mqttProxyUrl = s"tcp://${proxy.getContainerIpAddress}:${proxy.getProxyPort}"

      val source           = "mqttSourceTopic"
      val target           = "kafkaTopic"
      val sourcesToConvMap = Map(source -> new BytesConverter)
      val settings = MqttSourceSettings(
        mqttProxyUrl,
        None,
        None,
        UUID.randomUUID().toString,
        sourcesToConvMap.map { case (k, v) => k -> v.getClass.getCanonicalName },
        throwOnConversion = true,
        Array(s"INSERT INTO $target SELECT * FROM $source"),
        1,
        1000,
        1000,
        cleanSession = true,
        1000,
        None,
        None,
        None,
      )

      // Instantiate the mqtt manager. It will subscribe to topic $source, at the currently active broker.
      val mqttManager = new MqttManager(MqttClientConnectionFn.apply, sourcesToConvMap, settings)
      Thread.sleep(2000)

      // The broker "crashes" and loses all state. The mqttManager should reconnect and resubscribe.
      mqttContainer.stop()

      // A new broker starts up. The MqttManager should now reconnect and resubscribe.
      mqttContainer.start()
      Thread.sleep(5000)

      // Publish a message to the topic the manager should have resubscribed to.
      val message = "message1"

      val msg = new MqttMessage(message.getBytes())
      // message should be delivered once
      msg.setQos(2)
      msg.setRetained(false)

      val client = new MqttClient(mqttProxyUrl, UUID.randomUUID.toString)
      client.connect()
      client.publish(source, msg)
      client.disconnect()
      client.close()

      Thread.sleep(2000)

      val records = new util.LinkedList[SourceRecord]()
      mqttManager.getRecords(records)

      // Verify that the records were received by the manager.
      try {
        records.size() shouldBe 1
        records.get(0).topic() shouldBe target
        records.get(0).value() shouldBe message.getBytes()
        records.get(0).valueSchema() shouldBe Schema.BYTES_SCHEMA
      } finally {
        mqttManager.close()
      }
    }
  }

}
