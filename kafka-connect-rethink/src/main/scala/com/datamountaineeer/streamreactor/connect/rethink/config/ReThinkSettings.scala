/**
  * Copyright 2016 Datamountaineer.
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

package com.datamountaineeer.streamreactor.connect.rethink.config

import com.datamountaineer.connector.config.{Config, WriteModeEnum}
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}
import org.apache.kafka.common.config.AbstractConfig

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 13/05/16.
  * stream-reactor-maven
  */
case class ReThinkSetting(routes: List[Config],
                          topicTableMap : Map[String, String],
                          fieldMap : Map[String, Map[String, String]],
                          conflictPolicy: Map[String, String],
                          errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                          maxRetries : Int,
                          batchSize : Int
                         )

//case class ReThinkSettings(settings : List[ReThinkSetting])

object ReThinkSettings {
  def apply(config: AbstractConfig, assigned: List[String], sinkTask: Boolean) : ReThinkSetting = {

    val raw = config.getString(ReThinkSinkConfig.EXPORT_ROUTE_QUERY)
    require((raw != null && !raw.isEmpty),  s"No ${ReThinkSinkConfig.EXPORT_ROUTE_QUERY} provided!")

    //parse query
    val routes: Set[Config] = raw.split(";").map(r => Config.parse(r)).toSet.filter(f=>assigned.contains(f.getSource))

    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(ReThinkSinkConfig.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val maxRetries = config.getInt(ReThinkSinkConfig.NBR_OF_RETRIES)
    val batchSize = config.getInt(ReThinkSinkConfig.BATCH_SIZE)

    //check conflict policy
    val conflictMap = routes.map(m=>{
      (m.getSource,
      m.getWriteMode match {
        case WriteModeEnum.INSERT => ReThinkSinkConfig.CONFLICT_ERROR
        case WriteModeEnum.UPSERT => ReThinkSinkConfig.CONFLICT_REPLACE
      })
    }).toMap

    val topicTableMap = routes.map(rm=>(rm.getSource, rm.getTarget)).toMap

    val fieldMap = routes.map({
      rm=>(rm.getSource,
        rm.getFieldAlias.map({
          fa=>(fa.getField,fa.getAlias)
        }).toMap)
    }).toMap

    ReThinkSetting(routes.toList, topicTableMap, fieldMap, conflictMap, errorPolicy, maxRetries, batchSize)
  }
}



