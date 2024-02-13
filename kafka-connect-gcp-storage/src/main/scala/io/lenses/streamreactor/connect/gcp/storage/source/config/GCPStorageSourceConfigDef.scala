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
package io.lenses.streamreactor.connect.gcp.storage.source.config

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.CloudConfigDef
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceSettingsKeys
import io.lenses.streamreactor.connect.gcp.storage.config.CommonConfigDef
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings.CONNECTOR_PREFIX
import org.apache.kafka.common.config.ConfigDef

object GCPStorageSourceConfigDef extends CommonConfigDef with CloudSourceSettingsKeys {

  override def connectorPrefix: String = CONNECTOR_PREFIX

  override val config: ConfigDef = {
    val settings = super.config

    addSourceOrderingSettings(settings)
    addSourcePartitionSearcherSettings(settings)
    addSourcePartitionExtractorSettings(settings)
  }
}

class GCPStorageSourceConfigDef() extends CloudConfigDef(CONNECTOR_PREFIX) with LazyLogging {}
