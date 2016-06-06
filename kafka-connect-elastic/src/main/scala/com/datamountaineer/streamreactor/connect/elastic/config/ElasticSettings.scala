/**
  * Copyright 2015 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.elastic.config

import com.datamountaineer.connector.config.Config
import org.apache.kafka.common.config.ConfigException
/**
  * Created by andrew@datamountaineer.com on 13/05/16. 
  * stream-reactor-maven
  */
case class ElasticSettings(routes: List[Config])

object ElasticSettings {
  def apply(config: ElasticSinkConfig, assigned : List[String]): ElasticSettings = {
    val raw = config.getString(ElasticSinkConfig.EXPORT_ROUTE_QUERY)
    require(!raw.isEmpty,s"Empty ${ElasticSinkConfig.EXPORT_ROUTE_QUERY_DOC}")

    val routes = raw.split(";").map(r=>Config.parse(r)).toList

    if (routes.size == 0) {
      throw new ConfigException(s"No routes for for assigned topics in ${ElasticSinkConfig.EXPORT_ROUTE_QUERY_DOC}")
    }
    ElasticSettings(routes)
  }
}
