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
package io.lenses.streamreactor.connect.cloud.config

import com.datamountaineer.streamreactor.common.config.base.traits.BaseSettings
import com.datamountaineer.streamreactor.common.config.base.traits.WithConnectorPrefix
import enumeratum.Enum
import enumeratum.EnumEntry

sealed trait DeleteModeOptions extends EnumEntry

object DeleteModeOptions extends Enum[DeleteModeOptions] {

  val values = findValues

  case object BatchDelete    extends DeleteModeOptions
  case object SeparateDelete extends DeleteModeOptions
}

trait DeleteModeConfigKeys extends WithConnectorPrefix {

  def DELETE_MODE: String = s"$connectorPrefix.delete.mode"

  val DELETE_MODE_DOC: String =
    "Cleaning index files for GCP Cloud Storage via the compatible S3 APIs requires individual delete requests. Options are `BatchDelete` or `SeparateDelete`. Defaults to `BatchDelete`."
  val DELETE_MODE_DEFAULT: String = "BatchDelete"

}

trait DeleteModeSettings extends BaseSettings with DeleteModeConfigKeys {

  def batchDelete(): Boolean =
    DeleteModeOptions.withNameInsensitive(getString(DELETE_MODE)) match {
      case DeleteModeOptions.BatchDelete    => true
      case DeleteModeOptions.SeparateDelete => false
    }
}
