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

package com.datamountaineer.streamreactor.connect.mongodb.config

import com.datamountaineer.streamreactor.connect.config.base.const.TraitConfigConst._

object MongoConfigConstants {

  val CONNECTOR_PREFIX= "connect.mongo"

  val DATABASE_CONFIG = s"$CONNECTOR_PREFIX.$DATABASE_PROP_SUFFIX"
  val DATABASE_CONFIG_DOC = "The mongodb target database."

  val CONNECTION_CONFIG = s"$CONNECTOR_PREFIX.connection"
  val CONNECTION_CONFIG_DOC = "The mongodb connection in the format mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]."

  val USERNAME_CONFIG = s"$CONNECTOR_PREFIX.$USERNAME_SUFFIX"
  val USERNAME_CONFIG_DEFAULT = ""
  val USERNAME_CONFIG_DOC = "The username to use when authenticating"

  val PASSWORD_CONFIG = s"$CONNECTOR_PREFIX.$PASSWORD_SUFFIX"
  val PASSWORD_CONFIG_DEFAULT = ""
  val PASSWORD_CONFIG_DOC = "The password for the use when authenticating"

  val AUTHENTICATION_MECHANISM = s"$CONNECTOR_PREFIX.$AUTH_MECH_SUFFIX"
  val AUTHENTICATION_MECHANISM_DEFAULT = "SCRAM-SHA-1"
  val AUTHENTICATION_MECHANISM_DOC =
    s"""
      |The authentication mechanism to use when username and password options are set. This can also be set in ${CONNECTION_CONFIG}" +
      |but requires the password to be exposed as plain text in the connection string which can leak in Connects logs."
    """.stringPrefix


  val BATCH_SIZE_CONFIG_DEFAULT = 100

  val ERROR_POLICY_CONFIG = s"$CONNECTOR_PREFIX.error.policy"
  val ERROR_POLICY_DOC: String = """
    Specifies the action to be taken if an error occurs while inserting the data.
      |There are two available options:
      |NOOP - the error is swallowed
      |THROW - the error is allowed to propagate.
      |RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on
      |The error will be logged automatically""".stripMargin

  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL_CONFIG = s"$CONNECTOR_PREFIX.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"

  val NBR_OF_RETRIES_CONFIG = s"$CONNECTOR_PREFIX.max.retries"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val KCQL_CONFIG = s"$CONNECTOR_PREFIX.kcql"
  val KCQL_DOC = "KCQL expression describing field selection and data routing to the target mongo db."

  val PROGRESS_COUNTER_ENABLED = "connect.progress.enabled"
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"
}
