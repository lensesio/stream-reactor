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

import com.datamountaineer.connector.config.Config
/**
  * Created by andrew@datamountaineer.com on 13/05/16. 
  * stream-reactor-maven
  */
case class ElasticSettings(kcql: Set[Config],
                           fields: Map[String, Map[String, String]],
                           ignoreFields: Map[String, Set[String]],
                           pks: Map[String, String],
                           tableMap: Map[String, String],
                           writeTimeout: Int = ElasticConfigConstants.WRITE_TIMEOUT_DEFAULT,
                           throwOnError: Boolean = ElasticConfigConstants.THROW_ON_ERROR_DEFAULT)

object ElasticSettings {

  def apply(config: ElasticConfig): ElasticSettings = {

    val kcql = config.getKCQL
    val fields = config.getFields()
    val tableMap = config.getTableTopic()
    val ignoreFields = config.getIgnoreFields()
    val pks = config.getUpsertKey()
    val writeTimeout = config.getWriteTimeout

    //TODO SHOULD THIS NOT BE THE STANDARD ERROR POLICY?????
    val throwOnError = config.getBoolean(ElasticConfigConstants.THROW_ON_ERROR_CONFIG)

    ElasticSettings(kcql = kcql,
      fields = fields,
      ignoreFields = ignoreFields,
      pks = pks,
      tableMap = tableMap,
      writeTimeout,
      throwOnError)

  }
}
