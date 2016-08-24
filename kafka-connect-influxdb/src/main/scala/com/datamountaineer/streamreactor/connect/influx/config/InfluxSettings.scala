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

package com.datamountaineer.streamreactor.connect.influx.config

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}
import com.datamountaineer.streamreactor.connect.influx.StructFieldsExtractor
import com.datamountaineer.streamreactor.connect.influx.config.InfluxSinkConfig._
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConversions._

case class InfluxSettings(connectionUrl: String,
                          user: String,
                          password: String,
                          database: String,
                          topicToMeasurementMap: Map[String, String],
                          fieldsExtractorMap: Map[String, StructFieldsExtractor],
                          errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                          maxRetries: Int = InfluxSinkConfig.NBR_OF_RETIRES_DEFAULT)

object InfluxSettings {

  /**
    * Creates an instance of InfluxSettings from a InfluxSinkConfig
    *
    * @param config : The map of all provided configurations
    * @return An instance of InfluxSettings
    */
  def apply(config: InfluxSinkConfig): InfluxSettings = {
    val url = config.getString(INFLUX_URL_CONFIG)

    if (url == null || url.trim.length == 0) {
      throw new ConfigException(s"${InfluxSinkConfig.INFLUX_URL_CONFIG} is not set correctly")
    }

    val user = config.getString(INFLUX_CONNECTION_USER_CONFIG)
    if (user == null || user.trim.length == 0) {
      throw new ConfigException(s"${InfluxSinkConfig.INFLUX_CONNECTION_USER_CONFIG} is not set correctly")
    }

    val password = config.getString(INFLUX_CONNECTION_PASSWORD_CONFIG)

    val database = config.getString(INFLUX_DATABASE_CONFIG)
    if (database == null || database.trim.isEmpty) {
      throw new ConfigException(s"$INFLUX_DATABASE_CONFIG is not set correctly")
    }
    val raw = config.getString(InfluxSinkConfig.EXPORT_ROUTE_QUERY_CONFIG)
    require(raw != null && !raw.isEmpty, s"No ${InfluxSinkConfig.EXPORT_ROUTE_QUERY_CONFIG} provided!")
    val routes = raw.split(";").map(r => Config.parse(r)).toSet
    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(InfluxSinkConfig.ERROR_POLICY_CONFIG).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val nbrOfRetries = config.getInt(InfluxSinkConfig.NBR_OF_RETRIES_CONFIG)

    val fields = routes.map(rm => (rm.getSource, rm.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap)).toMap

    val extractorFields = routes.map { rm =>
      val timestampField = Option(rm.getTimestamp) match {
        case Some(Config.TIMESTAMP) => None
        case other => other
      }
      (rm.getSource, StructFieldsExtractor(rm.isIncludeAllFields, fields(rm.getSource), timestampField))
    }.toMap

    new InfluxSettings(url,
      user,
      password,
      database,
      routes.map(r => r.getSource -> r.getTarget).toMap,
      extractorFields,
      errorPolicy,
      nbrOfRetries)
  }
}
