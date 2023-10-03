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
package io.lenses.streamreactor.connect.cloud.common.formats.reader

import com.google.common.io.ByteStreams
import io.lenses.streamreactor.connect.cloud.common.formats.bytes.BytesOutputRow

import java.io.InputStream
import scala.util.Try

class BytesStreamFileReader(
  input: InputStream,
  size:  Long,
) extends CloudDataIterator[BytesOutputRow] {

  private var consumed: Boolean = false

  override def hasNext: Boolean = !consumed && size > 0L

  override def next(): BytesOutputRow = {
    val fileAsBytes = ByteStreams.toByteArray(input)
    val row         = BytesOutputRow(fileAsBytes)
    consumed = true
    row
  }

  override def close(): Unit = {
    val _ = Try {
      input.close()
    }
  }

}
