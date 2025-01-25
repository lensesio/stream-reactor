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
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

trait SchemaChangeConfigKeys extends WithConnectorPrefix {

  protected def SCHEMA_CHANGE_DETECTOR = s"$connectorPrefix.schema.change.detector"

  private val SCHEMA_CHANGE_DETECTOR_DOC = "Schema change detector."
  private val SCHEMA_CHANGE_DETECTOR_DEFAULT: String = "default"

  def withSchemaChangeConfig(configDef: ConfigDef): ConfigDef =
    configDef.define(
      SCHEMA_CHANGE_DETECTOR,
      Type.STRING,
      SCHEMA_CHANGE_DETECTOR_DEFAULT,
      Importance.LOW,
      SCHEMA_CHANGE_DETECTOR_DOC,
      "Schema Change",
      1,
      ConfigDef.Width.MEDIUM,
      SCHEMA_CHANGE_DETECTOR,
    )
}

trait SchemaChangeSettings extends BaseSettings with SchemaChangeConfigKeys {

  def schemaChangeDetector(): SchemaChangeDetector =
    SchemaChangeDetector(getString(SCHEMA_CHANGE_DETECTOR))

}
