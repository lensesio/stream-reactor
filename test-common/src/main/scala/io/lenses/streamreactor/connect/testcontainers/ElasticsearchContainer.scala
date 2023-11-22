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

import io.lenses.streamreactor.connect.testcontainers.ElasticsearchContainer.defaultNetworkAlias
import io.lenses.streamreactor.connect.testcontainers.ElasticsearchContainer.defaultTag
import org.testcontainers.elasticsearch.{ ElasticsearchContainer => JavaElasticsearchContainer }
import org.testcontainers.utility.DockerImageName

class ElasticsearchContainer(
  dockerImage:      DockerImageName,
  dockerTag:        String = defaultTag,
  val networkAlias: String = defaultNetworkAlias,
) extends SingleContainer[JavaElasticsearchContainer] {

  val port: Int = 9200

  override val container: JavaElasticsearchContainer =
    new JavaElasticsearchContainer(dockerImage.withTag(dockerTag))
  container.withNetworkAliases(networkAlias)

  lazy val hostNetwork = new HostNetwork()

  class HostNetwork {
    def httpHostAddress: String = container.getHttpHostAddress
  }
}

object ElasticsearchContainer {
  private val dockerImage         = DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
  private val defaultTag          = "6.8.8"
  private val defaultNetworkAlias = "elastic"

  def apply(
    networkAlias: String = defaultNetworkAlias,
    dockerTag:    String = defaultTag,
  ): ElasticsearchContainer =
    new ElasticsearchContainer(dockerImage, dockerTag, networkAlias)
}
