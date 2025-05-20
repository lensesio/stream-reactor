/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.config

import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

trait EnableLatestSchemaOptimizationConfigKeys extends WithConnectorPrefix {

  def ENABLE_LATEST_SCHEMA_OPTIMIZATION = s"$connectorPrefix.latest.schema.optimization.enabled"

  val ENABLE_LATEST_SCHEMA_OPTIMIZATION_DOC =
    "If true, adapt records to the latest known schema before writing. Improves write performance by preventing flushes caused by compatible schema variations. Only use it if your schema evolution ensures backwards compatibility."
  private val ENABLE_LATEST_SCHEMA_OPTIMIZATION_DEFAULT: Boolean = false

  def withEnableLatestSchemaOptimizationConfig(configDef: ConfigDef): ConfigDef =
    configDef.define(
      ENABLE_LATEST_SCHEMA_OPTIMIZATION,
      Type.BOOLEAN,
      ENABLE_LATEST_SCHEMA_OPTIMIZATION_DEFAULT,
      Importance.LOW,
      ENABLE_LATEST_SCHEMA_OPTIMIZATION_DOC,
      "Data Flush Optimization",
      1,
      ConfigDef.Width.MEDIUM,
      ENABLE_LATEST_SCHEMA_OPTIMIZATION,
    )
}

trait EnableLatestSchemaOptimizationSettings extends BaseSettings with EnableLatestSchemaOptimizationConfigKeys {

  def getEnableLatestSchemaOptimization(): Boolean =
    getBoolean(ENABLE_LATEST_SCHEMA_OPTIMIZATION)

}
