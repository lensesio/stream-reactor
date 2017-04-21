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

import java.util

import com.datamountaineer.streamreactor.temp.{DatabaseSettings, KcqlSettings}
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

/**
  * Created by andrew@datamountaineer.com on 22/09/16. 
  * stream-reactor
  */
object ReThinkSourceConfig {
  val config: ConfigDef = new ConfigDef()
    .define(ReThinkSourceConfigConstants.RETHINK_HOST, Type.STRING,
      ReThinkSourceConfigConstants.RETHINK_HOST_DEFAULT,
      Importance.HIGH, ReThinkSourceConfigConstants.RETHINK_HOST_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, ReThinkSourceConfigConstants.RETHINK_HOST)
    .define(ReThinkSourceConfigConstants.RETHINK_DB, Type.STRING,
      ReThinkSourceConfigConstants.RETHINK_DB_DEFAULT,
      Importance.HIGH, ReThinkSourceConfigConstants.RETHINK_DB_DOC,
      "Connection", 2, ConfigDef.Width.MEDIUM, ReThinkSourceConfigConstants.RETHINK_DB)
    .define(ReThinkSourceConfigConstants.RETHINK_PORT, Type.INT,
      ReThinkSourceConfigConstants.RETHINK_PORT_DEFAULT,
      Importance.MEDIUM, ReThinkSourceConfigConstants.RETHINK_PORT_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, ReThinkSourceConfigConstants.RETHINK_PORT)
    .define(ReThinkSourceConfigConstants.IMPORT_ROUTE_QUERY, Type.STRING, Importance.HIGH,
      ReThinkSourceConfigConstants.IMPORT_ROUTE_QUERY,
      "Connection", 4, ConfigDef.Width.MEDIUM, ReThinkSourceConfigConstants.IMPORT_ROUTE_QUERY)
}

case class ReThinkSourceConfig(props: util.Map[String, String])
  extends AbstractConfig(ReThinkSourceConfig.config, props)
    with KcqlSettings
    with DatabaseSettings {
  override val kcqlConstant: String = ReThinkSourceConfigConstants.IMPORT_ROUTE_QUERY
  override val databaseConstant: String = ReThinkSourceConfigConstants.RETHINK_DB
}
