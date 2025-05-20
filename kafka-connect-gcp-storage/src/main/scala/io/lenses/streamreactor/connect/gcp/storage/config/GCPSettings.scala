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
package io.lenses.streamreactor.connect.gcp.storage.config

import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.common.config.source.ConfigSource
import io.lenses.streamreactor.common.utils.CyclopsToScalaEither
import io.lenses.streamreactor.connect.gcp.common.auth.GCPConnectionConfig
import io.lenses.streamreactor.connect.gcp.common.config.{ GCPSettings => JavaGCPSettings }
import org.apache.kafka.common.config.ConfigException

trait GCPSettings extends BaseSettings {

  private val javaGcpSettings = new JavaGCPSettings(javaConnectorPrefix)

  def getGcpConnectionSettings(config: ConfigSource): Either[Throwable, GCPConnectionConfig] =
    CyclopsToScalaEither.convertToScalaEither[ConfigException, GCPConnectionConfig](
      javaGcpSettings.parseFromConfig(config),
    );

}
