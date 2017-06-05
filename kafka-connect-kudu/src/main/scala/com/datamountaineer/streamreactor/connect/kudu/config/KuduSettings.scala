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

package com.datamountaineer.streamreactor.connect.kudu.config

import com.datamountaineer.connector.config.{Config, WriteModeEnum}
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 13/05/16. 
  * stream-reactor-maven
  */
case class KuduSettings(routes: List[Config],
                        topicTables: Map[String, String],
                        allowAutoCreate: Map[String, Boolean],
                        allowAutoEvolve: Map[String, Boolean],
                        fieldsMap: Map[String, Map[String, String]],
                        ignoreFields: Map[String, Set[String]],
                        writeModeMap: Map[String, WriteModeEnum],
                        errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                        maxRetries: Int = KuduSinkConfigConstants.NBR_OF_RETIRES_DEFAULT,
                        schemaRegistryUrl: String)

object KuduSettings {

  def apply(config: KuduSinkConfig): KuduSettings = {

    val raw = config.getString(KuduSinkConfigConstants.EXPORT_ROUTE_QUERY)
    require(raw.nonEmpty, s"No ${KuduSinkConfigConstants.EXPORT_ROUTE_QUERY} provided!")
    val routes = raw.split(";").map(r => Config.parse(r)).toSet
    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(KuduSinkConfigConstants.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val maxRetries = config.getInt(KuduSinkConfigConstants.NBR_OF_RETRIES)
    val autoCreate = routes.map(r => (r.getSource, r.isAutoCreate)).toMap
    val autoEvolve = routes.map(r => (r.getSource, r.isAutoEvolve)).toMap
    val schemaRegUrl = config.getString(KuduSinkConfigConstants.SCHEMA_REGISTRY_URL)

    val fieldsMap = routes.map(
      rm => (rm.getSource, rm.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap)
    ).toMap

    val ignoreFields = routes.map(r => (r.getSource, r.getIgnoredField.toSet)).toMap
    val writeModeMap = routes.map(r => (r.getSource, r.getWriteMode)).toMap
    val topicTables = routes.map(r => (r.getSource, r.getTarget)).toMap

    new KuduSettings(routes = routes.toList,
      topicTables = topicTables,
      allowAutoCreate = autoCreate,
      allowAutoEvolve = autoEvolve,
      fieldsMap = fieldsMap,
      ignoreFields = ignoreFields,
      writeModeMap = writeModeMap,
      errorPolicy = errorPolicy,
      maxRetries = maxRetries,
      schemaRegistryUrl = schemaRegUrl)
  }
}


