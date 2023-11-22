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
import org.testcontainers.containers.Network

abstract class SingleContainer[T <: GenericContainer[_]] {

  def container: T

  def start(): Unit = container.start()

  def stop(): Unit = container.stop()

  def withNetwork(network: Network): this.type = {
    container.withNetwork(network)
    this
  }

  def withExposedPorts(ports: Integer*): this.type = {
    container.withExposedPorts(ports: _*)
    this
  }
}
