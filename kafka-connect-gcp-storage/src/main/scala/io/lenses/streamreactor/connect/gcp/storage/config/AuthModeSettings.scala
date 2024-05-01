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

import io.lenses.streamreactor.common.config.base.ConfigMap
import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.connect.gcp.common.auth.mode.AuthMode
import io.lenses.streamreactor.connect.gcp.common.config.{ AuthModeSettings => JavaAuthModeSettings }

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Try

trait AuthModeSettings extends BaseSettings {

  private val javaAuthModeSettings = new JavaAuthModeSettings(javaConnectorPrefix)

  def getAuthMode(props: Map[String, AnyRef]): Either[Throwable, AuthMode] = {
    val configMap = new ConfigMap(props.asJava)
    Try(javaAuthModeSettings.parseFromConfig(configMap)).toEither
  }

}
