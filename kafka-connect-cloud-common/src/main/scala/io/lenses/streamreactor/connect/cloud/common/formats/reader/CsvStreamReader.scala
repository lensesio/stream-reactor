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
package io.lenses.streamreactor.connect.cloud.common.formats.reader

import io.lenses.streamreactor.connect.cloud.common.formats.FormatWriterException
import java.io.InputStream

class CsvStreamReader(input: InputStream, hasHeaders: Boolean) extends TextStreamReader(input) {

  private var firstRun: Boolean = true

  override def next(): String = {
    if (firstRun) {
      if (hasHeaders) {
        if (sourceLines.hasNext) {
          sourceLines.next()
        } else {
          throw FormatWriterException("No column headers are available")
        }
      }
      firstRun = false
    }
    super.next()
  }

}
