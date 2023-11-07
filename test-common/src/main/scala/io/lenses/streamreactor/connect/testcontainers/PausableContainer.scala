package io.lenses.streamreactor.connect.testcontainers

import org.testcontainers.containers.GenericContainer

trait PausableContainer {

  def container: GenericContainer[_]

  def resume(): Unit = {
    val _ = container.getDockerClient.unpauseContainerCmd(container.getContainerId).exec()
  }

  def pause(): Unit = {
    val _ = container.getDockerClient.pauseContainerCmd(container.getContainerId).exec()
  }

}
