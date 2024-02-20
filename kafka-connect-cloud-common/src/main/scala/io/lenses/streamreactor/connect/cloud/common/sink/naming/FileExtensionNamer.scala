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
package io.lenses.streamreactor.connect.cloud.common.sink.naming

import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.config.FormatSelection
import io.lenses.streamreactor.connect.cloud.common.config.JsonFormatSelection

object FileExtensionNamer {

  /**
    * Reconciles the file extensions set in the given format & codec.
    *
    * @note Avro or Parquet do not change filenames when compressed; guards for JSON only.
    */
  def fileExtension(codec: CompressionCodec, formatSelection: FormatSelection): String = {
    if (formatSelection != JsonFormatSelection)
      return formatSelection.extension

    codec.extension.getOrElse(formatSelection.extension)
  }
}
