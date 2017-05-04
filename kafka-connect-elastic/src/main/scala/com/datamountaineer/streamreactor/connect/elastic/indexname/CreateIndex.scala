package com.datamountaineer.streamreactor.connect.elastic.indexname

import com.datamountaineer.connector.config.Config
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse

import scala.concurrent.Future

/**
  * Creates the index for the given KCQL configuration.
  */
object CreateIndex {
  def apply(config: Config)(implicit client: ElasticClient): Future[CreateIndexResponse] = {
    require(config.isAutoCreate, s"Auto-creating indexes hasn't been enabled for target:${config.getTarget}")

    val indexName = getIndexName(config)
    client.execute {
      Option(config.getDocType) match {
        case None => create index indexName
        case Some(documentType) => create index indexName mappings documentType
      }
    }
  }

  def getIndexName(config: Config): String = {
    Option(config.getIndexSuffix).fold(config.getTarget) { indexNameSuffix =>
      s"${config.getTarget}${CustomIndexName.parseIndexName(indexNameSuffix)}"
    }
  }
}
