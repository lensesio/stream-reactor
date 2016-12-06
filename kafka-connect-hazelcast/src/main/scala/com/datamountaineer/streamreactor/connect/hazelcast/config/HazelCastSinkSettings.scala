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

package com.datamountaineer.streamreactor.connect.hazelcast.config

import com.datamountaineer.connector.config.{Config, FormatType}
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}
import com.datamountaineer.streamreactor.connect.hazelcast.config.TargetType.TargetType
import org.apache.kafka.connect.errors.ConnectException

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * Created by andrew@datamountaineer.com on 08/08/16.
  * stream-reactor
  */

object TargetType extends Enumeration {
  type TargetType = Value
  val RELIABLE_TOPIC, RING_BUFFER = Value
}


case class HazelCastStoreAsType(name: String, targetType: TargetType)

case class HazelCastSinkSettings(groupName : String,
                                 connConfig: HazelCastConnectionConfig,
                                 routes: Set[Config],
                                 topicObject: Map[String, HazelCastStoreAsType],
                                 fieldsMap : Map[String, Map[String, String]],
                                 ignoreFields: Map[String, Set[String]],
                                 errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                                 maxRetries: Int = HazelCastSinkConfig.NBR_OF_RETIRES_DEFAULT,
                                 format: Map[String, FormatType])

object HazelCastSinkSettings {
  def apply(config: HazelCastSinkConfig): HazelCastSinkSettings = {
    val raw = config.getString(HazelCastSinkConfig.EXPORT_ROUTE_QUERY)
    require(raw.nonEmpty,  s"No ${HazelCastSinkConfig.EXPORT_ROUTE_QUERY} provided!")
    val routes = raw.split(";").map(r => Config.parse(r)).toSet
    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(HazelCastSinkConfig.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val maxRetries = config.getInt(HazelCastSinkConfig.NBR_OF_RETRIES)

    val topicTables = routes.map(r => {
      Try(TargetType.withName(r.getStoredAs.toUpperCase)) match {
        case Success(s) => (r.getSource, HazelCastStoreAsType(r.getTarget, TargetType.withName(r.getStoredAs.toUpperCase)))
        case Failure(f) => (r.getSource, HazelCastStoreAsType(r.getTarget, TargetType.RELIABLE_TOPIC))
      }
    }).toMap

    val fieldsMap = routes.map(
      rm => (rm.getSource, rm.getFieldAlias.map(fa => (fa.getField,fa.getAlias)).toMap)
    ).toMap

    val ignoreFields = routes.map(r => (r.getSource, r.getIgnoredField.toSet)).toMap
    val groupName = config.getString(HazelCastSinkConfig.SINK_GROUP_NAME)
    require(groupName.nonEmpty,  s"No ${HazelCastSinkConfig.SINK_GROUP_NAME} provided!")
    val connConfig = HazelCastConnectionConfig(config)
    val format = routes.map(r => (r.getSource, getFormatType(r.getFormatType))).toMap

    new HazelCastSinkSettings(groupName,
                              connConfig,
                              routes,
                              topicTables,
                              fieldsMap,
                              ignoreFields,
                              errorPolicy,
                              maxRetries,
                              format)
  }

  private def getFormatType(format: FormatType) = {
    if (format == null) {
      FormatType.JSON
    } else {
      format match {
        case FormatType.AVRO | FormatType.JSON | FormatType.TEXT =>
        case _ => throw new ConnectException(s"Unknown WITHFORMAT type")
      }
      format
    }
  }
}


