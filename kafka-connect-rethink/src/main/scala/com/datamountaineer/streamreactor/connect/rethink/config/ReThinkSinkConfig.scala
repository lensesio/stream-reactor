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

import com.datamountaineer.streamreactor.connect.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

/**
  * Created by andrew@datamountaineer.com on 24/03/16. 
  * stream-reactor
  */
object ReThinkSinkConfig extends ReThinkConfig {

  val config: ConfigDef = baseConfig
    .define(ReThinkConfigConstants.ERROR_POLICY, Type.STRING,
      ReThinkConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH, ReThinkConfigConstants.ERROR_POLICY_DOC,
      "Connection", 9, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.ERROR_POLICY)

    .define(ReThinkConfigConstants.ERROR_RETRY_INTERVAL, Type.INT,
      ReThinkConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM, ReThinkConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Connection", 10, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.ERROR_RETRY_INTERVAL)

    .define(ReThinkConfigConstants.NBR_OF_RETRIES, Type.INT,
      ReThinkConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM, ReThinkConfigConstants.NBR_OF_RETRIES_DOC,
      "Connection", 11, ConfigDef.Width.MEDIUM, ReThinkConfigConstants.NBR_OF_RETRIES)
}

case class ReThinkSinkConfig(props: util.Map[String, String])
  extends BaseConfig(ReThinkConfigConstants.RETHINK_CONNECTOR_PREFIX, ReThinkSinkConfig.config, props)
    with ErrorPolicySettings
    with NumberRetriesSettings
    with KcqlSettings
    with DatabaseSettings
    with RetryIntervalSettings
