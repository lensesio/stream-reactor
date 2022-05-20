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

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.common.errors.ErrorPolicy
import com.datamountaineer.streamreactor.common.errors.ThrowErrorPolicy
import org.apache.kafka.common.config.ConfigException
import org.influxdb.InfluxDB.ConsistencyLevel

case class InfluxSettings(
  connectionUrl:    String,
  user:             String,
  password:         String,
  database:         String,
  retentionPolicy:  String,
  consistencyLevel: ConsistencyLevel,
  /*topicToMeasurementMap: Map[String, String],
                          fieldsExtractorMap: Map[String, StructFieldsExtractor],
                          topicToTagsMap: Map[String, Seq[Tag]]*/
  kcqls:       Seq[Kcql],
  errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
  maxRetries:  Int         = InfluxConfigConstants.NBR_OF_RETIRES_DEFAULT,
)

object InfluxSettings {

  /**
    * Creates an instance of InfluxSettings from a InfluxSinkConfig
    *
    * @param config : The map of all provided configurations
    * @return An instance of InfluxSettings
    */
  def apply(config: InfluxConfig): InfluxSettings = {
    val url = config.getString(InfluxConfigConstants.INFLUX_URL_CONFIG)

    if (url == null || url.trim.length == 0) {
      throw new ConfigException(s"${InfluxConfigConstants.INFLUX_URL_CONFIG} is not set correctly")
    }

    val user = config.getUsername

    if (user == null || user.trim.length == 0) {
      throw new ConfigException(s"${InfluxConfigConstants.INFLUX_CONNECTION_USER_CONFIG} is not set correctly")
    }

    val passwordRaw = config.getSecret

    val password = passwordRaw.value() match {
      case "" => null
      case _  => passwordRaw.value()
    }

    val database = config.getDatabase

    if (database == null || database.trim.isEmpty) {
      throw new ConfigException(s"${InfluxConfigConstants.INFLUX_DATABASE_CONFIG} is not set correctly")
    }

    //TODO: common lib should not return Set[Kcql] but Seq[Kcq;
    val errorPolicy  = config.getErrorPolicy
    val nbrOfRetries = config.getNumberRetries
    //val fields = config.getFields()

    /*val extractorFields = kcql.map { rm =>
      val timestampField = Option(rm.getTimestamp) match {
        case Some(Kcql.TIMESTAMP) => None
        case other => other
      }
      (rm.getSource, StructFieldsExtractor(rm.isIncludeAllFields, fields(rm.getSource), timestampField, rm.getIgnoredField.toSet))
    }.toMap*/

    val retentionPolicy = config.getString(InfluxConfigConstants.RETENTION_POLICY_CONFIG)

    val consistencyLevel = config.getConsistencyLevel.get

    new InfluxSettings(url,
                       user,
                       password,
                       database,
                       retentionPolicy,
                       consistencyLevel,
                       config.getKCQL.toVector,
                       errorPolicy,
                       nbrOfRetries,
    )
  }
}
