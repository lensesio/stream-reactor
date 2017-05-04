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

package com.datamountaineer.streamreactor.connect.elastic.config

import com.datamountaineer.connector.config.{Config, WriteModeEnum}

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 13/05/16. 
  * stream-reactor-maven
  */
case class ElasticSettings(routes: List[Config],
                           fields: Map[String, Map[String, String]],
                           ignoreFields: Map[String, Set[String]],
                           pks: Map[String, String],
                           tableMap: Map[String, String],
                           writeTimeout: Int = ElasticSinkConfigConstants.WRITE_TIMEOUT_DEFAULT,
                           throwOnError: Boolean = ElasticSinkConfigConstants.THROW_ON_ERROR_DEFAULT)

object ElasticSettings {

  def apply(config: ElasticSinkConfig): ElasticSettings = {
    val raw = config.getString(ElasticSinkConfigConstants.EXPORT_ROUTE_QUERY)
    require(!raw.isEmpty, s"Empty ${ElasticSinkConfigConstants.EXPORT_ROUTE_QUERY_DOC}")
    val routes = raw.split(";").map(r => Config.parse(r)).toList

    val fields = routes.map(
      rm => (rm.getSource, rm.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap)
    ).toMap

    val tableMap = routes.map(rm => (rm.getSource, rm.getTarget)).toMap
    val ignoreFields = routes.map(rm => (rm.getSource, rm.getIgnoredField.toSet)).toMap

    val pks = routes
      .filter(c => c.getWriteMode == WriteModeEnum.UPSERT)
      .map { r =>
        val keys = r.getPrimaryKeys.toSet
        require(keys.nonEmpty && keys.size == 1, s"${r.getTarget} is set up with upsert, you need one primary key setup")
        (r.getSource, keys.head)
      }.toMap

    val writeTimeout = config.getInt(ElasticSinkConfigConstants.WRITE_TIMEOUT_CONFIG)

    val throwOnError = config.getBoolean(ElasticSinkConfigConstants.THROW_ON_ERROR_CONFIG)
    ElasticSettings(routes = routes,
      fields = fields,
      ignoreFields = ignoreFields,
      pks = pks,
      tableMap = tableMap,
      writeTimeout,
      throwOnError)

  }
}
