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

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object RedisSinkConfig {

  val REDIS_HOST = "connect.redis.connection.host"
  val REDIS_HOST_DOC =
    """
      |Specifies the redis server
    """.stripMargin

  val REDIS_PORT = "connect.redis.connection.port"
  val REDIS_PORT_DOC =
    """
      |Specifies the redis connection port
    """.stripMargin

  val REDIS_PASSWORD = "connect.redis.connection.password"
  val REDIS_PASSWORD_DOC =
    """
      |Provides the password for the redis connection.
    """.stripMargin

  val EXPORT_ROUTE_QUERY = "connect.hbase.export.route.query"
  val EXPORT_ROUTE_QUERY_DOC = ""

  val ERROR_POLICY = "connect.hbase.error.policy"
  val ERROR_POLICY_DOC = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
    "There are two available options: \n" + "NOOP - the error is swallowed \n" +
    "THROW - the error is allowed to propagate. \n" +
    "RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on \n" +
    "The error will be logged automatically";
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = "connect.hbase.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES = "connect.hbase.max.retires"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val config: ConfigDef = new ConfigDef()
    .define(REDIS_HOST, Type.STRING, Importance.HIGH, REDIS_HOST_DOC)
    .define(REDIS_PORT, Type.INT, Importance.HIGH, REDIS_PORT_DOC)
    .define(REDIS_PASSWORD, Type.PASSWORD, Importance.LOW, REDIS_PASSWORD_DOC)
    .define(EXPORT_ROUTE_QUERY, Type.STRING, Importance.HIGH, EXPORT_ROUTE_QUERY)
    .define(ERROR_POLICY, Type.STRING, ERROR_POLICY_DEFAULT, Importance.HIGH, ERROR_POLICY_DOC)
    .define(ERROR_RETRY_INTERVAL, Type.INT, ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, ERROR_RETRY_INTERVAL_DOC)
    .define(NBR_OF_RETRIES, Type.INT, NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, NBR_OF_RETRIES_DOC)
}

/**
  * <h1>RedisSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
class RedisSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(RedisSinkConfig.config, props)
