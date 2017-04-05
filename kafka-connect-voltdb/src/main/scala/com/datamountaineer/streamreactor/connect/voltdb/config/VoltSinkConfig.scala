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

package com.datamountaineer.streamreactor.connect.voltdb.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object VoltSinkConfig {
  val SERVERS_CONFIG = "connect.volt.connection.servers"
  val SERVERS_DOC = "Comma separated server[:port]"

  val USER_CONFIG = "connect.volt.connection.user"
  val USER_DOC = "The user to connect to the volt database"

  val PASSWORD_CONFIG = "connect.volt.connection.password"
  val PASSWORD_DOC = "The password for the voltdb user."

  val EXPORT_ROUTE_QUERY_CONFIG = "connect.volt.sink.kcql"
  val EXPORT_ROUTE_QUERY_DOC = "KCQL expression describing field selection and routes."

  val ERROR_POLICY_CONFIG = "connect.volt.error.policy"
  val ERROR_POLICY_DOC: String = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
    "There are two available options: \n" + "NOOP - the error is swallowed \n" +
    "THROW - the error is allowed to propagate. \n" +
    "RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on \n" +
    "The error will be logged automatically"
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL_CONFIG = "connect.volt.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES_CONFIG = "connect.volt.max.retries"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val config: ConfigDef = new ConfigDef()
    .define(SERVERS_CONFIG, Type.STRING, Importance.HIGH, SERVERS_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, SERVERS_CONFIG)
    .define(USER_CONFIG, Type.STRING, Importance.HIGH, USER_DOC,
      "Connection", 2, ConfigDef.Width.MEDIUM, USER_CONFIG)
    .define(PASSWORD_CONFIG, Type.PASSWORD, Importance.HIGH, PASSWORD_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, PASSWORD_CONFIG)
    .define(EXPORT_ROUTE_QUERY_CONFIG, Type.STRING, Importance.HIGH, EXPORT_ROUTE_QUERY_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, EXPORT_ROUTE_QUERY_CONFIG)
    .define(ERROR_POLICY_CONFIG, Type.STRING, ERROR_POLICY_DEFAULT, Importance.HIGH, ERROR_POLICY_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, ERROR_POLICY_CONFIG)
    .define(ERROR_RETRY_INTERVAL_CONFIG, Type.INT, ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM,
      ERROR_RETRY_INTERVAL_DOC, "Connection", 1, ConfigDef.Width.MEDIUM, ERROR_RETRY_INTERVAL_CONFIG)
    .define(NBR_OF_RETRIES_CONFIG, Type.INT, NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, NBR_OF_RETRIES_DOC,
      "Connection", 6, ConfigDef.Width.MEDIUM, NBR_OF_RETRIES_CONFIG)
}

/**
  * <h1>VoltSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
case class VoltSinkConfig(props: util.Map[String, String]) extends AbstractConfig(VoltSinkConfig.config, props)
