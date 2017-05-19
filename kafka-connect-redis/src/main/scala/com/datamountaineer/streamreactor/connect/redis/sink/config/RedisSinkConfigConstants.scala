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

package com.datamountaineer.streamreactor.connect.redis.sink.config


object RedisSinkConfigConstants {
  val REDIS_HOST = "connect.redis.connection.host"
  private[config] val REDIS_HOST_DOC: String =
    """
      |Specifies the redis server
    """.stripMargin

  val REDIS_PORT = "connect.redis.connection.port"
  private[config] val REDIS_PORT_DOC: String =
    """
      |Specifies the redis connection port
    """.stripMargin

  val REDIS_PASSWORD = "connect.redis.connection.password"
  private[config] val REDIS_PASSWORD_DOC: String =
    """
      |Provides the password for the redis connection.
    """.stripMargin

  val KCQL_CONFIG = "connect.redis.sink.kcql"
  private[config] val KCQL_DOC = "KCQL expression describing field selection and routes."

  val ERROR_POLICY = "connect.redis.error.policy"
  private[config] val ERROR_POLICY_DOC: String =
    """Specifies the action to be taken if an error occurs while inserting the data.
      |There are two available options:
      |NOOP - the error is swallowed
      |THROW - the error is allowed to propagate.
      |RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on
      |The error will be logged automatically""".stripMargin
  private[config] val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = "connect.redis.retry.interval"
  private[config] val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  private[config] val ERROR_RETRY_INTERVAL_DEFAULT = "60000"

  val NBR_OF_RETRIES = "connect.redis.max.retries"
  private[config] val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  private[config] val NBR_OF_RETIRES_DEFAULT = 20

  val PROGRESS_COUNTER_ENABLED = "connect.progress.enabled"
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"
}
