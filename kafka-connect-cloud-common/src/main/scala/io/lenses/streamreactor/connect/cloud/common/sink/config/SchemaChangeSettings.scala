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
package io.lenses.streamreactor.connect.cloud.common.sink.config

import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

trait SchemaChangeConfigKeys extends WithConnectorPrefix {

  protected def SCHEMA_CHANGE_ROLLOVER = s"$connectorPrefix.schema.change.rollover"

  private val SCHEMA_CHANGE_ROLLOVER_DOC = "Roll over on schema change."
  private val SCHEMA_CHANGE_ROLLOVER_DEFAULT: Boolean = true

  def withSchemaChangeConfig(configDef: ConfigDef): ConfigDef =
    configDef.define(
      SCHEMA_CHANGE_ROLLOVER,
      Type.BOOLEAN,
      SCHEMA_CHANGE_ROLLOVER_DEFAULT,
      Importance.LOW,
      SCHEMA_CHANGE_ROLLOVER_DOC,
      "Schema Change",
      1,
      ConfigDef.Width.SHORT,
      SCHEMA_CHANGE_ROLLOVER,
    )
}

trait SchemaChangeSettings extends BaseSettings with SchemaChangeConfigKeys {

  def shouldRollOverOnSchemaChange(): Boolean =
    getBoolean(SCHEMA_CHANGE_ROLLOVER)

}
