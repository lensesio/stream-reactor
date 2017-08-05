package com.datamountaineer.streamreactor.connect.elastic

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.elastic.config.{ClientType, ElasticSettings}
import com.datamountaineer.streamreactor.connect.elastic.indexname.CreateIndex.getIndexName
import com.sksamuel.elastic4s.bulk.{BulkDefinition, RichBulkResponse}

import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.mappings.MappingDefinition
import com.sksamuel.elastic4s.xpack.security.XPackElasticClient
import com.sksamuel.elastic4s.{ElasticsearchClientUri, TcpClient}
import org.elasticsearch.common.settings.Settings

import scala.concurrent.Future

trait KElasticClient extends AutoCloseable {
  def index(kcql: Config)

  def execute(definition: BulkDefinition): Future[Any]
}


object KElasticClient {
  def getClient(settings: ElasticSettings, essettings: Settings, uri: ElasticsearchClientUri): KElasticClient = {
    if (settings.clientType.equals(ClientType.HTTP)) {
      new HttpKElasticClient(HttpClient(uri))
    }
    else if (settings.xPackSettings.nonEmpty) {
      new TcpKElasticClient(XPackElasticClient(essettings, uri, settings.xpackPluggins: _*))
    } else {
      new TcpKElasticClient(TcpClient.transport(essettings, uri))
    }
  }

}

class TcpKElasticClient(client: TcpClient) extends KElasticClient {
  import com.sksamuel.elastic4s.ElasticDsl._
  override def index(config: Config): Unit = {
    require(config.isAutoCreate, s"Auto-creating indexes hasn't been enabled for target:${config.getTarget}")

    val indexName = getIndexName(config)
    client.execute {
      Option(config.getDocType) match {
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
  override def index(config: Config): Unit = {
    require(config.isAutoCreate, s"Auto-creating indexes hasn't been enabled for target:${config.getTarget}")

    val indexName = getIndexName(config)
    client.execute {
      Option(config.getDocType) match {
        case None => createIndex(indexName)
        case Some(documentType) => createIndex(indexName).mappings(MappingDefinition(documentType))
      }
    }
  }

  override def execute(definition: BulkDefinition): Future[Any] = client.execute(definition)

  override def close(): Unit = client.close()
}
