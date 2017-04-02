/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.influx.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object InfluxSinkConfig {
  val INFLUX_URL_CONFIG = "connect.influx.connection.url"
  val INFLUX_URL_DOC = "The InfluxDB database url."

  val INFLUX_DATABASE_CONFIG = "connect.influx.connection.database"
  val INFLUX_DATABASE_DOC = "The database to store the values to."

  val INFLUX_CONNECTION_USER_CONFIG = "connect.influx.connection.user"
  val INFLUX_CONNECTION_USER_DOC = "The user to connect to the influx database"

  val INFLUX_CONNECTION_PASSWORD_CONFIG = "connect.influx.connection.password"
  val INFLUX_CONNECTION_PASSWORD_DOC = "The password for the influxdb user."


  val RETENTION_POLICY_CONFIG = "connect.influx.retention.policy"
  val RETENTION_POLICY_DOC: String =
    """
      |Determines how long InfluxDB keeps the data - the options for specifying the duration of the retention policy are listed below.
      |Note that the minimum retention period is one hour.
      |DURATION determines how long InfluxDB keeps the data - the options for specifying the duration of the retention policy are listed below. Note that the minimum retention period is one hour.
      |m minutes
      |h hours
      |d days
      |w weeks
      |INF infinite
      |
      |Default retention is `autogen` from 1.0 onwards or `default` for any previous version""".stripMargin
  val RETENTION_POLICY_DEFAULT = "autogen"

  val KCQL_CONFIG = "connect.influx.sink.kcql"
  val KCQL_DOC = "KCQL expression describing field selection and routes."
  val KCQL_DISPLAY = "Kafka Connect Query Language"

  val ERROR_POLICY_CONFIG = "connect.influx.error.policy"
  val ERROR_POLICY_DOC: String = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
    "There are two available options: \n" + "NOOP - the error is swallowed \n" +
    "THROW - the error is allowed to propagate. \n" +
    "RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on \n" +
    "The error will be logged automatically"
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL_CONFIG = "connect.influx.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES_CONFIG = "connect.influx.max.retries"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val CONSISTENCY_CONFIG = "connect.influx.consistency.level"
  val CONSISTENCY_DOC = "Specifies the write consistency. If any write operations do not meet the configured consistency guarantees, an error will occur and the data will not be indexed. The default consistency-level is ALL."
  val CONSISTENCY_DEFAULT = "ALL"
  val CONSISTENCY_DISPLAY = "Consistency Level"

  val config: ConfigDef = new ConfigDef()
    .define(INFLUX_URL_CONFIG, Type.STRING, Importance.HIGH, INFLUX_URL_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, INFLUX_URL_CONFIG)
    .define(INFLUX_DATABASE_CONFIG, Type.STRING, Importance.HIGH, INFLUX_DATABASE_DOC,
      "Connection", 2, ConfigDef.Width.MEDIUM, INFLUX_DATABASE_CONFIG)
    .define(INFLUX_CONNECTION_USER_CONFIG, Type.STRING, Importance.HIGH, INFLUX_CONNECTION_USER_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, INFLUX_CONNECTION_USER_CONFIG)
    .define(INFLUX_CONNECTION_PASSWORD_CONFIG, Type.PASSWORD, "", Importance.HIGH, INFLUX_CONNECTION_PASSWORD_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, INFLUX_CONNECTION_PASSWORD_CONFIG)
    .define(KCQL_CONFIG, Type.STRING, Importance.HIGH, KCQL_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, KCQL_DISPLAY)

    .define(ERROR_POLICY_CONFIG, Type.STRING, ERROR_POLICY_DEFAULT, Importance.HIGH, ERROR_POLICY_DOC,
      "Miscellaneous", 1, ConfigDef.Width.MEDIUM, ERROR_POLICY_CONFIG)
    .define(ERROR_RETRY_INTERVAL_CONFIG, Type.INT, ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, ERROR_RETRY_INTERVAL_DOC,
      "Miscellaneous", 2, ConfigDef.Width.MEDIUM, ERROR_RETRY_INTERVAL_CONFIG)
    .define(NBR_OF_RETRIES_CONFIG, Type.INT, NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, NBR_OF_RETRIES_DOC,
      "Miscellaneous", 3, ConfigDef.Width.MEDIUM, NBR_OF_RETRIES_CONFIG)

    .define(RETENTION_POLICY_CONFIG, Type.STRING, RETENTION_POLICY_DEFAULT, Importance.HIGH, RETENTION_POLICY_DOC,
      "Writes", 1, ConfigDef.Width.MEDIUM, RETENTION_POLICY_DOC)
    .define(CONSISTENCY_CONFIG, Type.STRING, CONSISTENCY_DEFAULT, Importance.MEDIUM, CONSISTENCY_DOC,
      "Writes", 2, ConfigDef.Width.MEDIUM, CONSISTENCY_DISPLAY)
}

/**
  * <h1>InfluxSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
case class InfluxSinkConfig(props: util.Map[String, String]) extends AbstractConfig(InfluxSinkConfig.config, props)
