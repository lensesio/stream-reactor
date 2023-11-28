/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.azure.documentdb.config

import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.CONSISTENCY_LEVEL_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.DATABASE_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.ERROR_POLICY_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.KCQL_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.MAX_RETRIES_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.PROGRESS_ENABLED_CONST
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.RETRY_INTERVAL_PROP_SUFFIX

/**
  * Holds the constants used in the config.
  */

object DocumentDbConfigConstants {
  val CONNECTOR_PREFIX = "connect.documentdb"

  val DATABASE_CONFIG     = s"$CONNECTOR_PREFIX.$DATABASE_PROP_SUFFIX"
  val DATABASE_CONFIG_DOC = "The Azure DocumentDb target database."

  val CONNECTION_CONFIG     = s"$CONNECTOR_PREFIX.endpoint"
  val CONNECTION_CONFIG_DOC = "The Azure DocumentDb end point."
  val CONNECTION_DISPLAY    = "Connection endpoint."

  val MASTER_KEY_CONFIG = s"$CONNECTOR_PREFIX.master.key"
  val MASTER_KEY_DOC    = "The connection master key"

  val ERROR_POLICY_CONFIG = s"$CONNECTOR_PREFIX.${ERROR_POLICY_PROP_SUFFIX}"
  val ERROR_POLICY_DOC: String =
    """Specifies the action to be taken if an error occurs while inserting the data
      |There are two available options:
      |NOOP - the error is swallowed
      |THROW - the error is allowed to propagate.
      |RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on
      |The error will be logged automatically""".stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL_CONFIG  = s"$CONNECTOR_PREFIX.${RETRY_INTERVAL_PROP_SUFFIX}"
  val ERROR_RETRY_INTERVAL_DOC     = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"

  val NBR_OF_RETRIES_CONFIG  = s"$CONNECTOR_PREFIX.${MAX_RETRIES_PROP_SUFFIX}"
  val NBR_OF_RETRIES_DOC     = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val KCQL_CONFIG = s"$CONNECTOR_PREFIX.${KCQL_PROP_SUFFIX}"
  val KCQL_DOC    = "KCQL expression describing field selection and data routing to the target DocumentDb."

  val CONSISTENCY_CONFIG = s"$CONNECTOR_PREFIX.${CONSISTENCY_LEVEL_PROP_SUFFIX}"
  val CONSISTENCY_DOC =
    "Determines the write visibility. There are four possible values: Strong,BoundedStaleness,Session or Eventual"
  val CONSISTENCY_DISPLAY = "Writes consistency"
  val CONSISTENCY_DEFAULT = "Session"

  val CREATE_DATABASE_CONFIG = s"$CONNECTOR_PREFIX.${DATABASE_PROP_SUFFIX}.create"
  val CREATE_DATABASE_DOC =
    "If set to true it will create the database if it doesn't exist. If this is set to default(false) an exception will be raised."
  val CREATE_DATABASE_DISPLAY = "Auto-create database"
  val CREATE_DATABASE_DEFAULT = false

  val PROXY_HOST_CONFIG  = s"$CONNECTOR_PREFIX.proxy"
  val PROXY_HOST_DOC     = "Specifies the connection proxy details."
  val PROXY_HOST_DISPLAY = "Proxy URI"

  val PROGRESS_COUNTER_ENABLED         = PROGRESS_ENABLED_CONST
  val PROGRESS_COUNTER_ENABLED_DOC     = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"
}
