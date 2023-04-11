/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package com.landoop.streamreactor.connect.hive.utils

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigException

object AbstractConfigExtension {

  implicit class AbstractConfigExtensions(val config: AbstractConfig) extends AnyVal {
    def getStringOrThrowIfNull(key: String): String =
      Option(config.getString(key))
        .getOrElse {
          throw new ConfigException(s"Missing the configuration for [$key].")
        }

    def getPasswordOrThrowIfNull(key: String): String =
      Option(config.getPassword(key))
        .map(_.value())
        .getOrElse {
          throw new ConfigException(s"Missing the configuration for [$key].")
        }
  }

}
