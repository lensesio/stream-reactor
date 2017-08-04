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

import java.util

import com.datamountaineer.streamreactor.connect.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

object HBaseConfig {
  val config: ConfigDef = new ConfigDef()
    .define(HBaseConfigConstants.COLUMN_FAMILY, Type.STRING, Importance.HIGH, HBaseConfigConstants.COLUMN_FAMILY_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, HBaseConfigConstants.COLUMN_FAMILY)
    .define(HBaseConfigConstants.KCQL_QUERY, Type.STRING, Importance.HIGH, HBaseConfigConstants.KCQL_QUERY,
      "Connection", 2, ConfigDef.Width.MEDIUM, HBaseConfigConstants.KCQL_QUERY)
    .define(HBaseConfigConstants.ERROR_POLICY, Type.STRING, HBaseConfigConstants.ERROR_POLICY_DEFAULT, Importance.HIGH, HBaseConfigConstants.ERROR_POLICY_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, HBaseConfigConstants.ERROR_POLICY)
    .define(HBaseConfigConstants.ERROR_RETRY_INTERVAL, Type.INT, HBaseConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, HBaseConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, HBaseConfigConstants.ERROR_RETRY_INTERVAL)
    .define(HBaseConfigConstants.NBR_OF_RETRIES, Type.INT, HBaseConfigConstants.NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, HBaseConfigConstants.NBR_OF_RETRIES_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, HBaseConfigConstants.NBR_OF_RETRIES)
    .define(HBaseConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, HBaseConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM, HBaseConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, HBaseConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
}

/**
  * <h1>HbaseSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
case class HBaseConfig(props: util.Map[String, String])
  extends BaseConfig(HBaseConfigConstants.CONNECTOR_PREFIX, HBaseConfig.config, props)
    with KcqlSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
