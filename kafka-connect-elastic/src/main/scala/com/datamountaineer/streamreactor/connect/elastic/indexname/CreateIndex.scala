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

package com.datamountaineer.streamreactor.connect.elastic.indexname

import com.datamountaineer.kcql.Kcql
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse

import scala.concurrent.Future

/**
  * Creates the index for the given KCQL configuration.
  */
object CreateIndex {
  def apply(config: Kcql)(implicit client: ElasticClient): Future[CreateIndexResponse] = {
    require(config.isAutoCreate, s"Auto-creating indexes hasn't been enabled for target:${config.getTarget}")

    val indexName = getIndexName(config)
    client.execute {
      Option(config.getDocType) match {
        case None => create index indexName
        case Some(documentType) => create index indexName mappings (mapping(documentType))
      }
    }
  }

  def getIndexName(config: Kcql): String = {
    Option(config.getIndexSuffix).fold(config.getTarget) { indexNameSuffix =>
      s"${config.getTarget}${CustomIndexName.parseIndexName(indexNameSuffix)}"
    }
  }
}
