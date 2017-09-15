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

package com.datamountaineer.streamreactor.connect.druid.config

import com.datamountaineer.streamreactor.connect.config.base.const.TraitConfigConst.{KCQL_PROP_SUFFIX, PROGRESS_ENABLED_CONST}

object DruidSinkConfigConstants {
  val CONNECTOR_PREFIX = "connect.druid"
  val KCQL = s"${CONNECTOR_PREFIX}.${KCQL_PROP_SUFFIX}"
  val KCQL_DOC = "The KCQL statement to specify field selection from topics and druid datasource targets."

  val CONFIG_FILE = s"${CONNECTOR_PREFIX}.config.file"
  val CONFIG_FILE_DOC = "The path to the configuration file."

  val TIMEOUT = s"${CONNECTOR_PREFIX}.write.timeout"
  val TIMEOUT_DOC: String =
    """
      |Specifies the number of seconds to wait for the write to Druid to happen.
    """.stripMargin
  val TIMEOUT_DEFAULT = 6000

  val PROGRESS_COUNTER_ENABLED = s"${PROGRESS_ENABLED_CONST}"
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"
}
