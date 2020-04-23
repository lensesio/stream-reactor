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

package com.datamountaineer.streamreactor.connect.rethink.config

import com.datamountaineer.streamreactor.connect.config.base.const.TraitConfigConst._

object ReThinkConfigConstants {
  val RETHINK_CONNECTOR_PREFIX = "connect.rethink"

  val RETHINK_HOST = s"$RETHINK_CONNECTOR_PREFIX.host"
  val RETHINK_HOST_DOC = "Rethink server host."
  val RETHINK_HOST_DEFAULT = "localhost"
  val RETHINK_DB = s"$RETHINK_CONNECTOR_PREFIX.$DATABASE_PROP_SUFFIX"

  val RETHINK_DB_DEFAULT = "connect_rethink_sink"
  val RETHINK_DB_DOC = "The reThink database to read from."

  val RETHINK_PORT = s"$RETHINK_CONNECTOR_PREFIX.port"
  val RETHINK_PORT_DEFAULT = "28015"
  val RETHINK_PORT_DOC = "Client port of rethink server to connect to."
  val KCQL = s"$RETHINK_CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"
  val KCQL_DOC = "The KCQL expression for the connector."

  val PROGRESS_COUNTER_ENABLED = "connect.progress.enabled"
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

  val USERNAME = s"$RETHINK_CONNECTOR_PREFIX.rethink.username"
  val USERNAME_DOC = "The user name to connect to rethink with."
  val USERNAME_DEFAULT= ""

  val PASSWORD = s"$RETHINK_CONNECTOR_PREFIX.password"
  val PASSWORD_DOC = "The password for the user."
  val PASSWORD_DEFAULT = ""

  val CERT_FILE = s"$RETHINK_CONNECTOR_PREFIX.rethink.cert.file"
  val CERT_FILE_DOC = "Certificate file to use for secure TLS connection to the rethinkdb servers. Cannot be used with username/password."
  val CERT_FILE_DEFAULT = ""

  val AUTH_KEY = s"$RETHINK_CONNECTOR_PREFIX.rethink.auth.key"
  val AUTH_KEY_DOC = "The authorization key to use in combination with the certificate file."
  val AUTH_KEY_DEFAULT = ""

  val CONFLICT_ERROR = "error"
  val CONFLICT_REPLACE = "replace"
  val CONFLICT_UPDATE = "update"

  val ERROR_POLICY = s"$RETHINK_CONNECTOR_PREFIX.$ERROR_POLICY_PROP_SUFFIX"
  val ERROR_POLICY_DOC: String = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
    "There are two available options: \n" + "NOOP - the error is swallowed \n" +
    "THROW - the error is allowed to propagate. \n" +
    "RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on \n" +
    "The error will be logged automatically"
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = s"$RETHINK_CONNECTOR_PREFIX.$RETRY_INTERVAL_PROP_SUFFIX"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"

  val NBR_OF_RETRIES = s"$RETHINK_CONNECTOR_PREFIX.$MAX_RETRIES_PROP_SUFFIX"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val SOURCE_LINGER_MS = s"$RETHINK_CONNECTOR_PREFIX.linger.ms"
  val SOURCE_LINGER_MS_DEFAULT = 5000L
  val SOURCE_LINGER_MS_DOC = "The number of milliseconds to wait before flushing the received messages to Kafka. The records will" +
    "be flushed if the batch size is reached before the linger period has expired."

  val BATCH_SIZE_DEFAULT = 1000
}
