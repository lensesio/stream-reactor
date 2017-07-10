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

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object HbaseSinkConfig {
  val config: ConfigDef = new ConfigDef()
    .define(HbaseSinkConfigConstants.COLUMN_FAMILY, Type.STRING, Importance.HIGH, HbaseSinkConfigConstants.COLUMN_FAMILY_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, HbaseSinkConfigConstants.COLUMN_FAMILY)
    .define(HbaseSinkConfigConstants.EXPORT_ROUTE_QUERY, Type.STRING, Importance.HIGH, HbaseSinkConfigConstants.EXPORT_ROUTE_QUERY,
      "Connection", 2, ConfigDef.Width.MEDIUM, HbaseSinkConfigConstants.EXPORT_ROUTE_QUERY)
    .define(HbaseSinkConfigConstants.ERROR_POLICY, Type.STRING, HbaseSinkConfigConstants.ERROR_POLICY_DEFAULT, Importance.HIGH, HbaseSinkConfigConstants.ERROR_POLICY_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, HbaseSinkConfigConstants.ERROR_POLICY)
    .define(HbaseSinkConfigConstants.ERROR_RETRY_INTERVAL, Type.INT, HbaseSinkConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, HbaseSinkConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, HbaseSinkConfigConstants.ERROR_RETRY_INTERVAL)
    .define(HbaseSinkConfigConstants.NBR_OF_RETRIES, Type.INT, HbaseSinkConfigConstants.NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, HbaseSinkConfigConstants.NBR_OF_RETRIES_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, HbaseSinkConfigConstants.NBR_OF_RETRIES)
    .define(HbaseSinkConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, HbaseSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM, HbaseSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, HbaseSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
}

/**
  * <h1>HbaseSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
case class HbaseSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(HbaseSinkConfig.config, props)
