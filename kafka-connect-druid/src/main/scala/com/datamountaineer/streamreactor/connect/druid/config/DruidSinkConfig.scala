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

  val config: ConfigDef = new ConfigDef()
    .define(DruidSinkConfigConstants.KCQL, Type.STRING, Importance.HIGH, DruidSinkConfigConstants.KCQL_DOC, "Connection", 1, ConfigDef.Width.MEDIUM, DruidSinkConfigConstants.KCQL)
    .define(DruidSinkConfigConstants.CONFIG_FILE, Type.STRING, Importance.HIGH, DruidSinkConfigConstants.CONFIG_FILE_DOC, "Connection", 2, ConfigDef.Width.LONG, DruidSinkConfigConstants.CONFIG_FILE)
    .define(DruidSinkConfigConstants.TIMEOUT, Type.INT, DruidSinkConfigConstants.TIMEOUT_DEFAULT, Importance.LOW, DruidSinkConfigConstants.TIMEOUT_DOC, "Connection", 3, ConfigDef.Width.SHORT, DruidSinkConfigConstants.TIMEOUT)
    .define(DruidSinkConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, DruidSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT, Importance.MEDIUM, DruidSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DOC, "Metrics", 1, ConfigDef.Width.MEDIUM, DruidSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
}

/**
  * <h1>DruidSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
class DruidSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(DruidSinkConfig.config, props)
