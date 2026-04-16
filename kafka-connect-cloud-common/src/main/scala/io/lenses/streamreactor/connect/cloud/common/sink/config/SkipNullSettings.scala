/*
 * Copyright 2017-2026 Lenses.io Ltd
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

trait SkipNullConfigKeys extends WithConnectorPrefix {

  protected def SKIP_NULL_VALUES = s"$connectorPrefix.skip.null.values"

  private val SKIP_NULL_VALUES_DOC = "Skip null values."
  private val SKIP_NULL_VALUES_DEFAULT: Boolean = false

  def withSkipNullConfig(configDef: ConfigDef): ConfigDef =
    configDef.define(
      SKIP_NULL_VALUES,
      Type.BOOLEAN,
      SKIP_NULL_VALUES_DEFAULT,
      Importance.LOW,
      SKIP_NULL_VALUES_DOC,
      "Value handling",
      1,
      ConfigDef.Width.SHORT,
      SKIP_NULL_VALUES,
    )
}

trait SkipNullSettings extends BaseSettings with SkipNullConfigKeys {

  def skipNullValues(): Boolean = getBoolean(SKIP_NULL_VALUES)

}
