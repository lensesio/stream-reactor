/*
 * Copyright 2020 Lenses.io
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

package com.datamountaineer.streamreactor.connect.elastic7

import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import org.testcontainers.elasticsearch.ElasticsearchContainer

object CreateLocalNodeClientUtil {

  private val url = "docker.elastic.co/elasticsearch/elasticsearch:7.2.0"

  def createLocalNode() = {
    val container = new ElasticsearchContainer(url)
    //container.withReuse(true)
    container.start()
    container
  }

  def createLocalNodeClient(localNode: ElasticsearchContainer) = {
    val esProps = ElasticProperties(s"http://${localNode.getHttpHostAddress}")
    val client = ElasticClient(JavaClient(esProps))
    client
  }
}
