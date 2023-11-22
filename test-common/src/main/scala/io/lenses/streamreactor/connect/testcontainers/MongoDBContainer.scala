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
import cats.effect.kernel.Resource
import com.mongodb.MongoClient
import io.lenses.streamreactor.connect.testcontainers.MongoDBContainer.defaultNetworkAlias
import io.lenses.streamreactor.connect.testcontainers.MongoDBContainer.defaultTag
import org.testcontainers.containers.{ MongoDBContainer => JavaMongoDBContainer }
import org.testcontainers.utility.DockerImageName

class MongoDBContainer(
  dockerImage:      DockerImageName,
  dockerTag:        String = defaultTag,
  val networkAlias: String = defaultNetworkAlias,
) extends SingleContainer[JavaMongoDBContainer] {

  val port: Int = 27017

  override val container: JavaMongoDBContainer =
    new JavaMongoDBContainer(dockerImage.withTag(dockerTag))
  container.withNetworkAliases(networkAlias)

  lazy val hostNetwork = new HostNetwork()

  class HostNetwork {
    val mongoClient = Resource.fromAutoCloseable(IO(new MongoClient(container.getHost, container.getMappedPort(port))))
  }
}

object MongoDBContainer {
  private val dockerImage         = DockerImageName.parse("mongo")
  private val defaultTag          = "4.0.10"
  private val defaultNetworkAlias = "mongo"

  def apply(
    networkAlias: String = defaultNetworkAlias,
    dockerTag:    String = defaultTag,
  ): MongoDBContainer =
    new MongoDBContainer(dockerImage, dockerTag, networkAlias)
}
