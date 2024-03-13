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

import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.KCQL_PROP_SUFFIX
import org.apache.kafka.common.config.ConfigException

trait KcqlSettings extends BaseSettings {
  val kcqlConstant: String = s"$connectorPrefix.$KCQL_PROP_SUFFIX"

  def getKCQL: Set[Kcql] =
    Kcql.parseMultiple(getKCQLString).asScala.toSet

  private def getKCQLString: String = {
    val raw = getString(kcqlConstant)
    if (raw.isEmpty) {
      throw new ConfigException(s"Missing [$kcqlConstant]")
    }
    raw
  }

}
