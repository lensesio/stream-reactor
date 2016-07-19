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

package com.datamountaineer.streamreactor.connect.redis.sink.config

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}
import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkConfig._
import com.datamountaineer.streamreactor.connect.rowkeys._
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


/**
  * Holds the Redis Sink settings
  */
case class RedisSinkSettings(connection: RedisConnectionInfo,
                             rowKeyModeMap : Map[String, StringKeyBuilder],
                             routes: List[Config],
                             fields: Map[String, Map[String, String]],
                             ignoreFields : Map[String, Set[String]],
                             errorPolicy : ErrorPolicy = new ThrowErrorPolicy,
                             taskRetries : Int = RedisSinkConfig.NBR_OF_RETIRES_DEFAULT)


object RedisSinkSettings {
  def apply(config: RedisSinkConfig): RedisSinkSettings = {
    val raw = config.getString(RedisSinkConfig.EXPORT_ROUTE_QUERY)
    require(raw != null && !raw.isEmpty, s"No ${RedisSinkConfig.EXPORT_ROUTE_QUERY} provided!")
    val routes = raw.split(";").map(r => Config.parse(r)).toSet
    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(RedisSinkConfig.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val nbrOfRetries = config.getInt(RedisSinkConfig.NBR_OF_RETRIES)

    val rowKeyModeMap = routes.map(r => {
        val keys = r.getPrimaryKeys.asScala.toList
        if (keys.nonEmpty) (r.getSource, StringStructFieldsStringKeyBuilder(keys)) else (r.getSource, new StringGenericRowKeyBuilder())
      }
    ).toMap

    val fieldsMap = routes.map(
      rm => (rm.getSource, rm.getFieldAlias.map(fa => (fa.getField,fa.getAlias)).toMap)
    ).toMap

    val ignoreFields = routes.map(r => (r.getSource, r.getIgnoredField.asScala.toSet)).toMap
    val conn = RedisConnectionInfo(config)
    new RedisSinkSettings(conn, rowKeyModeMap, routes.toList, fieldsMap, ignoreFields, errorPolicy, nbrOfRetries)
  }
}

object RedisConnectionInfo {
  def apply(config: RedisSinkConfig): RedisConnectionInfo = {
    val host = config.getString(REDIS_HOST)
    if (host.isEmpty) new ConfigException(s"$REDIS_HOST is not set correctly")

    val passw = config.getString(REDIS_PASSWORD)
    val pass = if (passw.isEmpty) None else Some(passw)

    new RedisConnectionInfo(
      host,
      config.getInt(REDIS_PORT),
      pass)
  }
}

/**
  * Holds the Redis connection details.
  */
case class RedisConnectionInfo(host:String, port:Int, password:Option[String])
