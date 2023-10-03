package io.lenses.streamreactor.connect.testcontainers

import org.testcontainers.containers.GenericContainer

trait PausableContainer {

  def resume(): Unit

  def pause(): Unit

  def start(): Unit

  def stop(): Unit

  def getEndpointUrl(): String
}

class TestContainersPausableContainer(container: GenericContainer[_]) extends PausableContainer {

  def resume(): Unit = {
    val _ = container.getDockerClient.unpauseContainerCmd(container.getContainerId).exec()
  }

  def pause(): Unit = {
    val _ = container.getDockerClient.pauseContainerCmd(container.getContainerId).exec()
  }

  override def start(): Unit = container.start()

  override def stop(): Unit = container.stop()

  override def getEndpointUrl(): String = s"http://${container.getHost}:${container.getFirstMappedPort}"
}
