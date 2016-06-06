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

import java.net.ConnectException

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.config.{Helpers, RouteMapping}
import org.apache.kafka.common.config.AbstractConfig

/**
  * Created by andrew@datamountaineer.com on 13/05/16.
  * stream-reactor-maven
  */
case class ReThinkSetting(routes: List[Config], conflictPolicy: Map[String, String])

//case class ReThinkSettings(settings : List[ReThinkSetting])

object ReThinkSettings {
  def apply(config: AbstractConfig, assigned: List[String], sinkTask: Boolean) : ReThinkSetting = {

    val raw = config.getString(ReThinkSinkConfig.EXPORT_ROUTE_QUERY)
    require((raw != null && !raw.isEmpty),  s"No ${ReThinkSinkConfig.EXPORT_ROUTE_QUERY} provided!")

    //parse query
    val routes: Set[Config] = raw.split(";").map(r => Config.parse(r)).toSet.filter(f=>assigned.contains(f.getSource))

    val conflict = config.getString(ReThinkSinkConfig.CONFLICT_POLICY_MAP)
    val conflictMap: Map[String, String] = Helpers.tableTopicParser(conflict)

    //check conflict policy
    conflictMap.foreach({
      case (table, policy) => {
         if (!policy.equals(ReThinkSinkConfig.CONFLICT_ERROR) &&
          !policy.equals(ReThinkSinkConfig.CONFLICT_REPLACE) &&
          !policy.equals(ReThinkSinkConfig.CONFLICT_UPDATE)) {
          throw new ConnectException(s"Invalid conflict policy $policy set for table $table.")
        }
      }
    })

    ReThinkSetting(routes.toList, conflictMap)
  }
}



