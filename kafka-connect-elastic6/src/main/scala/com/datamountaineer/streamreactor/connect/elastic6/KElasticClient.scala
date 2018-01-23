/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.elastic6

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.elastic6.config.ElasticSettings
import com.datamountaineer.streamreactor.connect.elastic6.indexname.CreateIndex.getIndexName
import com.datamountaineer.streamreactor.connect.elastic6.config.ClientType
import com.sksamuel.elastic4s.bulk.BulkDefinition
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.mappings.MappingDefinition
import com.sksamuel.elastic4s.xpack.security.XPackElasticClient
import com.sksamuel.elastic4s.{ElasticsearchClientUri, TcpClient}
import org.elasticsearch.common.settings.Settings

import scala.concurrent.Future

trait KElasticClient extends AutoCloseable {
  def index(kcql: Kcql)

  def execute(definition: BulkDefinition): Future[Any]
}


object KElasticClient {
  def getClient(settings: ElasticSettings, essettings: Settings, uri: ElasticsearchClientUri): KElasticClient = {
    if (settings.clientType.equals(ClientType.HTTP)) {
      new HttpKElasticClient(HttpClient(uri))
    }
    else if (settings.xPackSettings.nonEmpty) {
      new TcpKElasticClient(XPackElasticClient(essettings, uri, settings.xPackPlugins: _*))
    } else {
      new TcpKElasticClient(TcpClient.transport(essettings, uri))
    }
  }

}

class TcpKElasticClient(client: TcpClient) extends KElasticClient {

  import com.sksamuel.elastic4s.ElasticDsl._

  override def index(kcql: Kcql): Unit = {
    require(kcql.isAutoCreate, s"Auto-creating indexes hasn't been enabled for target:${kcql.getTarget}")

    val indexName = getIndexName(kcql)
    client.execute {
      Option(kcql.getDocType) match {
        case None => createIndex(indexName)
        case Some(documentType) => createIndex(indexName).mappings(MappingDefinition(documentType))
      }
    }
  }

  override def execute(definition: BulkDefinition): Future[Any] = client.execute(definition)

  override def close(): Unit = client.close()

}

class HttpKElasticClient(client: HttpClient) extends KElasticClient {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def index(kcql: Kcql): Unit = {
    require(kcql.isAutoCreate, s"Auto-creating indexes hasn't been enabled for target:${kcql.getTarget}")

    val indexName = getIndexName(kcql)
    client.execute {
      Option(kcql.getDocType) match {
        case None => createIndex(indexName)
        case Some(documentType) => createIndex(indexName).mappings(MappingDefinition(documentType))
      }
    }
  }

  override def execute(definition: BulkDefinition): Future[Any] = client.execute(definition)

  override def close(): Unit = client.close()
}
