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

package com.datamountaineer.streamreactor.connect.rethink.config

import com.datamountaineer.connector.config.{Config, WriteModeEnum}
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}
import org.apache.kafka.connect.errors.ConnectException

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 13/05/16.
  * stream-reactor-maven
  */
case class ReThinkSinkSetting(db : String,
                              routes: Set[Config],
                              topicTableMap : Map[String, String],
                              fieldMap : Map[String, Map[String, String]],
                              ignoreFields: Map[String, Set[String]],
                              pks : Map[String, Set[String]],
                              conflictPolicy: Map[String, String],
                              errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                              maxRetries : Int,
                              batchSize : Int
                         )

object ReThinkSinkSettings {
  def apply(config: ReThinkSinkConfig) : ReThinkSinkSetting = {
    val raw = config.getString(ReThinkSinkConfig.EXPORT_ROUTE_QUERY)
    require(raw != null && !raw.isEmpty,  s"No ${ReThinkSinkConfig.EXPORT_ROUTE_QUERY} provided!")
    val routes = raw.split(";").map(r => Config.parse(r)).toSet

    //only allow on primary key for rethink.
    routes
      .filter(r => r.getPrimaryKeys.size > 1)
      .foreach(r => new ConnectException(s"More than one primary key found in ${ReThinkSinkConfig.EXPORT_ROUTE_QUERY}." +
        s" Only one field can be set."))

    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(ReThinkSinkConfig.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val maxRetries = config.getInt(ReThinkSinkConfig.NBR_OF_RETRIES)
    val batchSize = config.getInt(ReThinkSinkConfig.BATCH_SIZE)

    //check conflict policy
    val conflictMap = routes.map(m=>{
      (m.getTarget, m.getWriteMode match {
        case WriteModeEnum.INSERT => ReThinkSinkConfig.CONFLICT_ERROR
        case WriteModeEnum.UPSERT => ReThinkSinkConfig.CONFLICT_REPLACE
      })
    }).toMap

    val topicTableMap = routes.map(rm => (rm.getSource, rm.getTarget)).toMap

    val fieldMap = routes.map(
      rm => (rm.getSource, rm.getFieldAlias.map( fa => (fa.getField,fa.getAlias)).toMap)
    ).toMap

    val db = config.getString(ReThinkSinkConfig.RETHINK_DB)
    val p = routes.map(r => (r.getSource, r.getPrimaryKeys.toSet)).toMap

    //get the field expected in the sink record which maps to a primary key
    val pks = fieldMap.map({
      case (topic, fieldList) =>
        (topic,
        fieldList
          .filter({ case (f,a) => p.contains(a) })
          .map({ case (f, a) => f })
            .toSet)
    })
    val ignoreFields = routes.map(rm => (rm.getSource, rm.getIgnoredField.toSet)).toMap

    ReThinkSinkSetting(db, routes, topicTableMap, fieldMap, ignoreFields ,pks, conflictMap, errorPolicy, maxRetries, batchSize)
  }
}



