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

package com.datamountaineer.streamreactor.connect.influx.config

import com.datamountaineer.connector.config.{Config, Tag}
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}
import com.datamountaineer.streamreactor.connect.influx.StructFieldsExtractor
import org.apache.kafka.common.config.ConfigException
import org.influxdb.InfluxDB.ConsistencyLevel

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

case class InfluxSettings(connectionUrl: String,
                          user: String,
                          password: String,
                          database: String,
                          retentionPolicy: String,
                          consistencyLevel: ConsistencyLevel,
                          topicToMeasurementMap: Map[String, String],
                          fieldsExtractorMap: Map[String, StructFieldsExtractor],
                          topicToTagsMap: Map[String, Seq[Tag]],
                          errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                          maxRetries: Int = InfluxSinkConfigConstants.NBR_OF_RETIRES_DEFAULT)

object InfluxSettings {

  /**
    * Creates an instance of InfluxSettings from a InfluxSinkConfig
    *
    * @param config : The map of all provided configurations
    * @return An instance of InfluxSettings
    */
  def apply(config: InfluxSinkConfig): InfluxSettings = {
    val url = config.getString(InfluxSinkConfigConstants.INFLUX_URL_CONFIG)

    if (url == null || url.trim.length == 0) {
      throw new ConfigException(s"${InfluxSinkConfigConstants.INFLUX_URL_CONFIG} is not set correctly")
    }

    val user = config.getString(InfluxSinkConfigConstants.INFLUX_CONNECTION_USER_CONFIG)
    if (user == null || user.trim.length == 0) {
      throw new ConfigException(s"${InfluxSinkConfigConstants.INFLUX_CONNECTION_USER_CONFIG} is not set correctly")
    }

    val passwordRaw = config.getPassword(InfluxSinkConfigConstants.INFLUX_CONNECTION_PASSWORD_CONFIG)

    val password = passwordRaw match {
      case null => null
      case _ => passwordRaw.value()
    }

    val database = config.getString(InfluxSinkConfigConstants.INFLUX_DATABASE_CONFIG)
    if (database == null || database.trim.isEmpty) {
      throw new ConfigException(s"${InfluxSinkConfigConstants.INFLUX_DATABASE_CONFIG} is not set correctly")
    }
    val raw = config.getString(InfluxSinkConfigConstants.KCQL_CONFIG)
    require(raw != null && !raw.isEmpty, s"No ${InfluxSinkConfigConstants.KCQL_CONFIG} provided!")
    val kcql = raw.split(";").map(r => Config.parse(r)).toSet
    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(InfluxSinkConfigConstants.ERROR_POLICY_CONFIG).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val nbrOfRetries = config.getInt(InfluxSinkConfigConstants.NBR_OF_RETRIES_CONFIG)

    val fields = kcql.map(rm => (rm.getSource, rm.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap)).toMap

    val extractorFields = kcql.map { rm =>
      val timestampField = Option(rm.getTimestamp) match {
        case Some(Config.TIMESTAMP) => None
        case other => other
      }
      (rm.getSource, StructFieldsExtractor(rm.isIncludeAllFields, fields(rm.getSource), timestampField, rm.getIgnoredField.toSet))
    }.toMap

    val retentionPolicy = config.getString(InfluxSinkConfigConstants.RETENTION_POLICY_CONFIG)
    val consistencyLevel = Try {
      ConsistencyLevel.valueOf(config.getString(InfluxSinkConfigConstants.CONSISTENCY_CONFIG))
    } match {
      case Failure(_) => throw new ConfigException(s"${config.getString(InfluxSinkConfigConstants.CONSISTENCY_CONFIG)} is not a valid value for ${InfluxSinkConfigConstants.CONSISTENCY_CONFIG}. Available values are:${ConsistencyLevel.values().mkString(",")}")
      case Success(cl) => cl
    }

    new InfluxSettings(url,
      user,
      password,
      database,
      retentionPolicy,
      consistencyLevel,
      kcql.map(r => r.getSource -> r.getTarget).toMap,
      extractorFields,
      kcql.map { r => r.getSource -> r.getTags.toSeq }.filter(_._2.nonEmpty).toMap,
      errorPolicy,
      nbrOfRetries)
  }
}
