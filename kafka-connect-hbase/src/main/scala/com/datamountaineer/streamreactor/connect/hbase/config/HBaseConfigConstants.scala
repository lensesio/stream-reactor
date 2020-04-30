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

package com.datamountaineer.streamreactor.connect.hbase.config

import com.datamountaineer.streamreactor.connect.config.base.const.TraitConfigConst._
import com.datamountaineer.streamreactor.connect.hbase.kerberos.KerberosSettings

/**
  * Created by tomasfartaria on 10/04/2017.
  */
object HBaseConfigConstants extends KerberosSettings {

  val CONNECTOR_PREFIX = "connect.hbase"

  val COLUMN_FAMILY = s"${CONNECTOR_PREFIX}.column.family"
  val COLUMN_FAMILY_DOC = "The hbase column family."

  val KCQL_QUERY = s"${CONNECTOR_PREFIX}.${KCQL_PROP_SUFFIX}"
  val KCQL_DOC = "KCQL expression describing field selection and routes."

  val ERROR_POLICY = s"${CONNECTOR_PREFIX}.${ERROR_POLICY_PROP_SUFFIX}"
  val ERROR_POLICY_DOC: String =
    """Specifies the action to be taken if an error occurs while inserting the data.
      |There are two available options:
      |NOOP - the error is swallowed
      |THROW - the error is allowed to propagate.
      |RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on
      |The error will be logged automatically""".stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = s"${CONNECTOR_PREFIX}.${RETRY_INTERVAL_PROP_SUFFIX}"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"

  val NBR_OF_RETRIES = s"${CONNECTOR_PREFIX}.${MAX_RETRIES_PROP_SUFFIX}"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val PROGRESS_COUNTER_ENABLED: String = PROGRESS_ENABLED_CONST
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

  val HBASE_CONFIG_DIR = s"$CONNECTOR_PREFIX.conf.dir"
  val HBASE_CONFIG_DIR_DOC = "The HBase configuration directory."
  val HBASE_CONFIG_DIR_DEFAULT: String = null
  val HBASE_CONFIG_DIR_DISPLAY = "HBase Config Folder"

}
