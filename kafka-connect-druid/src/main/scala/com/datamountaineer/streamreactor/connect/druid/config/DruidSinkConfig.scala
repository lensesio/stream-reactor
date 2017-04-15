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

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object DruidSinkConfig {

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

  val config: ConfigDef = new ConfigDef()
    .define(KCQL, Type.STRING, Importance.HIGH, KCQL_DOC, "Connection", 1, ConfigDef.Width.MEDIUM, KCQL)
    .define(CONFIG_FILE, Type.STRING, Importance.HIGH, CONFIG_FILE_DOC, "Connection", 2, ConfigDef.Width.LONG, CONFIG_FILE)
    .define(TIMEOUT, Type.INT, TIMEOUT_DEFAULT, Importance.LOW, TIMEOUT_DOC, "Connection", 3, ConfigDef.Width.SHORT, TIMEOUT)
}

/**
  * <h1>DruidSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
class DruidSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(DruidSinkConfig.config, props)
