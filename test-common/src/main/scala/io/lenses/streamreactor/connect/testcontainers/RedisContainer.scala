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

import cats.effect.IO
import cats.effect.Resource
import io.lenses.streamreactor.connect.testcontainers.RedisContainer.defaultNetworkAlias
import io.lenses.streamreactor.connect.testcontainers.RedisContainer.defaultPort
import io.lenses.streamreactor.connect.testcontainers.RedisContainer.defaultTag
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import redis.clients.jedis.Jedis

class RedisContainer(
  dockerImage:      DockerImageName,
  dockerTag:        String = defaultTag,
  val networkAlias: String = defaultNetworkAlias,
  val port:         Int    = defaultPort,
) extends GenericContainer[RedisContainer](dockerImage.withTag(dockerTag)) {

  withNetworkAliases(networkAlias)
  withExposedPorts(port)

  lazy val hostNetwork = new HostNetwork()

  class HostNetwork {
    val jedisClient: Resource[IO, Jedis] = Resource.fromAutoCloseable(IO(new Jedis(getHost, getMappedPort(port))))
  }
}

object RedisContainer {
  private val dockerImage         = DockerImageName.parse("redis")
  private val defaultTag          = "6-alpine"
  private val defaultNetworkAlias = "redis"
  private val defaultPort         = 6379

  def apply(
    networkAlias: String = defaultNetworkAlias,
    dockerTag:    String = defaultTag,
    port:         Int    = defaultPort,
  ): RedisContainer =
    new RedisContainer(dockerImage, dockerTag, networkAlias, port)
}
