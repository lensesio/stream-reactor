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
package io.lenses.streamreactor.connect.gcp.storage.config

import com.datamountaineer.streamreactor.common.config.base.traits.BaseSettings
import com.datamountaineer.streamreactor.common.config.base.traits.WithConnectorPrefix
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type
trait UploadConfigKeys extends WithConnectorPrefix {

  /**
    * Resumable upload is a feature in Google Cloud Storage that allows the interruption and continuation of large file uploads, improving the reliability and efficiency of the upload process. When uploading large files, network issues or interruptions can occur, causing traditional uploads to fail and requiring the entire file to be re-uploaded.
    *
    * The resumable upload feature addresses this challenge by breaking the file into smaller, manageable chunks. If an interruption occurs, only the affected chunk needs to be re-uploaded, rather than the entire file. This not only reduces the likelihood of upload failures but also saves bandwidth and time.
    *
    * Disabling the resumable upload feature might be necessary in certain scenarios where the benefits of reduced bandwidth usage and upload efficiency are outweighed by specific application requirements. For instance, in environments with highly stable and uninterrupted network connections, or when dealing with small files, disabling resumable uploads could simplify the upload process without significant drawbacks. It provides flexibility for users to tailor the upload behavior based on their specific use case and network conditions.
    *
    * We enable this setting in the integration tests as the stubs we use are incapable of accepting such uploads.
    */
  protected val AVOID_RESUMABLE_UPLOAD: String = s"$connectorPrefix.avoid.resumable.upload"

  def addUploadSettingsToConfigDef(configDef: ConfigDef): ConfigDef =
    configDef.define(
      AVOID_RESUMABLE_UPLOAD,
      Type.BOOLEAN,
      "false",
      Importance.HIGH,
      "Avoid resumable uploads. Resumable upload in Google Cloud Storage enables the seamless continuation of large file uploads, preventing the need to re-upload the entire file in case of interruptions. Disabling this feature may be preferred in stable network conditions or when dealing with small files, streamlining the upload process based on specific application requirements. Default: Resumable uploads allowed.",
    )
}
trait UploadSettings extends BaseSettings with UploadConfigKeys {
  def isAvoidResumableUpload: Boolean = getBoolean(AVOID_RESUMABLE_UPLOAD)
}
