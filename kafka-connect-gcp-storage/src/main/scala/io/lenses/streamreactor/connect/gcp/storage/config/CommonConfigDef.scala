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
package io.lenses.streamreactor.connect.gcp.storage.config

import io.lenses.streamreactor.connect.cloud.common.config.CompressionCodecConfigKeys
import io.lenses.streamreactor.connect.gcp.common.config.GCPSettings
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

trait CommonConfigDef extends CompressionCodecConfigKeys {

  private val authModeSettingsConfigKeys: GCPSettings = new GCPSettings(javaConnectorPrefix)

  import authModeSettingsConfigKeys._

  def config: ConfigDef = {
    val conf = new ConfigDef()
      .define(
        KCQL_CONFIG,
        Type.STRING,
        Importance.HIGH,
        KCQL_DOC,
      )
      .define(
        ERROR_POLICY,
        Type.STRING,
        ERROR_POLICY_DEFAULT,
        Importance.HIGH,
        ERROR_POLICY_DOC,
        "Error",
        1,
        ConfigDef.Width.LONG,
        ERROR_POLICY,
      )
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
      .define(
        COMPRESSION_CODEC,
        Type.STRING,
        COMPRESSION_CODEC_DEFAULT,
        Importance.LOW,
        COMPRESSION_CODEC_DOC,
      )
      .define(
        COMPRESSION_LEVEL,
        Type.INT,
        COMPRESSION_LEVEL_DEFAULT,
        Importance.LOW,
        COMPRESSION_LEVEL_DOC,
      )
    withGCPSettings(conf)

  }
}
