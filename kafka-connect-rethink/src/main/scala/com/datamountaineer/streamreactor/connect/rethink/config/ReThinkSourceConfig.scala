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

import com.datamountaineer.streamreactor.connect.config.base.traits.{BaseConfig, BatchSizeSettings, DatabaseSettings, KcqlSettings}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

/**
  * Created by andrew@datamountaineer.com on 22/09/16. 
  * stream-reactor
  */
object ReThinkSourceConfig extends ReThinkConfig {
  val config: ConfigDef = baseConfig

    .define(ReThinkConfigConstants.SOURCE_LINGER_MS, Type.LONG,
      ReThinkConfigConstants.SOURCE_LINGER_MS_DEFAULT,
      Importance.MEDIUM, ReThinkConfigConstants.SOURCE_LINGER_MS_DOC,
      "Connection", 10, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.SOURCE_LINGER_MS)
}

case class ReThinkSourceConfig(props: util.Map[String, String])
  extends BaseConfig(ReThinkConfigConstants.RETHINK_CONNECTOR_PREFIX, ReThinkSourceConfig.config, props)
    with KcqlSettings
    with DatabaseSettings
    with BatchSizeSettings
