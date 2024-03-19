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
package io.lenses.streamreactor.connect.jms.config

/**
  * Created by andrew@datamountaineer.com on 31/07/2017.
  * stream-reactor
  */

import io.lenses.streamreactor.common.config.base.const.TraitConfigConst._
import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import org.apache.kafka.common.config.ConfigException

trait UrlSettings extends BaseSettings {
  private val urlConst = s"$connectorPrefix.$URL_SUFFIX"

  def getUrl: String = {
    val url = getString(urlConst)
    if (url == null || url.trim.isEmpty) {
      throw new ConfigException(s"$urlConst has not been set")
    }
    url
  }

}
