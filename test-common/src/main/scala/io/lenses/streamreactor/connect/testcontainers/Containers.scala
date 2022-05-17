package io.lenses.streamreactor.connect.testcontainers

import org.testcontainers.containers.{GenericContainer, Network}

abstract class SingleContainer[T <: GenericContainer[_]] {
  implicit def container: T

  def underlyingUnsafeContainer: T = container

  def start(): Unit = container.start()

  def stop(): Unit = container.stop()

  def withNetwork(network: Network): this.type = {
    container.withNetwork(network)
    this
  }
}
