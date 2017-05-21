package com.datamountaineer.streamreactor.connect.elastic.indexname

import com.datamountaineer.connector.config.Config
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.TcpClient
import com.sksamuel.elastic4s.mappings.MappingDefinition
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse

import scala.concurrent.Future

/**
  * Creates the index for the given KCQL configuration.
  */
object CreateIndex {
  def apply(config: Config)(implicit client: TcpClient): Future[CreateIndexResponse] = {
    require(config.isAutoCreate, s"Auto-creating indexes hasn't been enabled for target:${config.getTarget}")

    val indexName = getIndexName(config)
    client.execute {
      Option(config.getDocType) match {
        case None => createIndex(indexName)
        case Some(documentType) => createIndex(indexName).mappings(MappingDefinition(documentType))
      }
    }
  }

  def getIndexName(config: Config): String = {
    Option(config.getIndexSuffix).fold(config.getTarget) { indexNameSuffix =>
      s"${config.getTarget}${CustomIndexName.parseIndexName(indexNameSuffix)}"
    }
  }
}
