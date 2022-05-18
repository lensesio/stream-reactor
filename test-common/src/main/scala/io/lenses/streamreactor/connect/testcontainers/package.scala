package io.lenses.streamreactor.connect

import org.testcontainers.containers.KafkaContainer

package object testcontainers {

  implicit class KafkaContainerOps(kafkaContainer: KafkaContainer) {
    val kafkaPort = 9092
    def bootstrapServers: String =
      s"PLAINTEXT://${kafkaContainer.getNetworkAliases.get(0)}:$kafkaPort"
  }
}
