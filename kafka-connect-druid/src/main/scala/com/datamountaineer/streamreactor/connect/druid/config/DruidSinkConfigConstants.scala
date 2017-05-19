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

object DruidSinkConfigConstants {
  val KCQL = "connect.druid.sink.kcql"
  val KCQL_DOC = "The KCQL statement to specify field selection from topics and druid datasource targets."

  val CONFIG_FILE = "connect.druid.sink.config.file"
  val CONFIG_FILE_DOC = "The path to the configuration file."

  val TIMEOUT = "connnect.druid.sink.write.timeout"
  val TIMEOUT_DOC: String =
    """
      |Specifies the number of seconds to wait for the write to Druid to happen.
    """.stripMargin
  val TIMEOUT_DEFAULT = 6000

  val PROGRESS_COUNTER_ENABLED = "connect.progress.enabled"
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"
}
