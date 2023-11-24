/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
