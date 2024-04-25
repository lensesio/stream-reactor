/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.common.config.base.traits

import io.lenses.streamreactor.common.config.base.RetryConfig
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.MAX_RETRIES_PROP_SUFFIX
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.RETRY_INTERVAL_PROP_SUFFIX
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

trait ConnectorRetryConfigKeys extends WithConnectorPrefix {

  val NumRetries:    String = s"$connectorPrefix.$MAX_RETRIES_PROP_SUFFIX"
  val RetryInterval: String = s"$connectorPrefix.$RETRY_INTERVAL_PROP_SUFFIX"

  val ERROR_RETRY_INTERVAL     = s"$connectorPrefix.$RETRY_INTERVAL_PROP_SUFFIX"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT: Long = 60000L

  val NBR_OF_RETRIES     = s"$connectorPrefix.$MAX_RETRIES_PROP_SUFFIX"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT: Int = 20

  def withConnectorRetryConfig(configDef: ConfigDef): ConfigDef =
    configDef
      .define(
        NBR_OF_RETRIES,
        Type.INT,
        NBR_OF_RETIRES_DEFAULT,
        Importance.MEDIUM,
        NBR_OF_RETRIES_DOC,
        "Error",
        2,
        ConfigDef.Width.LONG,
        NBR_OF_RETRIES,
      )
      .define(
        ERROR_RETRY_INTERVAL,
        Type.LONG,
        ERROR_RETRY_INTERVAL_DEFAULT,
        Importance.MEDIUM,
        ERROR_RETRY_INTERVAL_DOC,
        "Error",
        3,
        ConfigDef.Width.LONG,
        ERROR_RETRY_INTERVAL,
      )
}

trait RetryConfigSettings extends BaseSettings with ConnectorRetryConfigKeys {

  private def getNumberRetries: Int = getInt(NumRetries)

  private def getRetryInterval: Long = getLong(RetryInterval)

  def getRetryConfig: RetryConfig =
    RetryConfig.builder().numberOfRetries(getNumberRetries).errorRetryInterval(getRetryInterval).build()

}
