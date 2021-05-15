package com.datamountaineer.streamreactor.connect.mqtt

import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.testcontainers.containers.GenericContainer

trait MqttTestContainer extends AnyWordSpec with Matchers with BeforeAndAfter {

  private val mqttPort = 1883

  protected val mqttContainer : GenericContainer[_] = new GenericContainer("eclipse-mosquitto:1.4.12")
    .withExposedPorts(mqttPort)

  before {
    mqttContainer.start()
  }

  after {
    mqttContainer.stop()
  }

  protected def getMqttConnectionUrl = s"tcp://${mqttContainer.getHost}:${mqttContainer.getMappedPort(mqttPort)}"
}
