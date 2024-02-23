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

import io.lenses.streamreactor.connect.cloud.common.config.CloudConfigDef
import io.lenses.streamreactor.connect.cloud.common.sink.config.FlushConfigKeys
import io.lenses.streamreactor.connect.cloud.common.sink.config.LocalStagingAreaConfigKeys
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingStrategyConfigKeys
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings.CONNECTOR_PREFIX
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings.SEEK_MAX_INDEX_FILES
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings.SEEK_MAX_INDEX_FILES_DEFAULT
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings.SEEK_MAX_INDEX_FILES_DOC
import io.lenses.streamreactor.connect.gcp.storage.config._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

object GCPStorageSinkConfigDef
    extends CommonConfigDef
    with FlushConfigKeys
    with LocalStagingAreaConfigKeys
    with PaddingStrategyConfigKeys
    with UploadConfigKeys {

  override def connectorPrefix: String = CONNECTOR_PREFIX

  override val config: ConfigDef = {
    val configDef = super.config
      .define(
        DISABLE_FLUSH_COUNT,
        Type.BOOLEAN,
        false,
        Importance.LOW,
        "Disable flush on reaching count",
      )
      .define(
        SEEK_MAX_INDEX_FILES,
        Type.INT,
        SEEK_MAX_INDEX_FILES_DEFAULT,
        Importance.LOW,
        SEEK_MAX_INDEX_FILES_DOC,
        "Sink Seek",
        2,
        ConfigDef.Width.LONG,
        SEEK_MAX_INDEX_FILES,
      )
    addLocalStagingAreaToConfigDef(configDef)
    addPaddingToConfigDef(configDef)
    addUploadSettingsToConfigDef(configDef)
  }

}

class GCPStorageSinkConfigDef() extends CloudConfigDef(CONNECTOR_PREFIX) {}
