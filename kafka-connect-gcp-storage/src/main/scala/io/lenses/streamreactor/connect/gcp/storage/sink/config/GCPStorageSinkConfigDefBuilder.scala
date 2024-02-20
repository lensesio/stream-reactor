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
package io.lenses.streamreactor.connect.gcp.storage.sink.config

import io.lenses.streamreactor.common.config.base.traits._
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkConfigDefBuilder
import io.lenses.streamreactor.connect.gcp.storage.config.AuthModeSettings
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings
import io.lenses.streamreactor.connect.gcp.storage.config.UploadSettings

import scala.jdk.CollectionConverters.MapHasAsScala

case class GCPStorageSinkConfigDefBuilder(props: Map[String, String])
    extends BaseConfig(GCPConfigSettings.CONNECTOR_PREFIX, GCPStorageSinkConfigDef.config, props)
    with CloudSinkConfigDefBuilder
    with ErrorPolicySettings
    with NumberRetriesSettings
    with UserSettings
    with ConnectionSettings
    with AuthModeSettings
    with UploadSettings {

  def getParsedValues: Map[String, _] = values().asScala.toMap

}
