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
package io.lenses.streamreactor.connect.aws.s3.formats.reader

import io.lenses.streamreactor.connect.aws.s3.formats.bytes.BytesOutputRow
import io.lenses.streamreactor.connect.aws.s3.formats.bytes.BytesWriteMode

import java.io.DataInputStream
import java.io.InputStream
import scala.util.Try

class BytesWithSizesStreamReader(
  input:          InputStream,
  size:           Long,
  bytesWriteMode: BytesWriteMode,
) extends S3DataIterator[BytesOutputRow] {

  private val inputStream = new DataInputStream(input)

  private var fileSizeCounter = size

  override def hasNext: Boolean = fileSizeCounter > 0

  override def next(): BytesOutputRow = {

    val row = bytesWriteMode.read(inputStream)
    fileSizeCounter -= row.bytesRead.get
    row
  }

  override def close(): Unit = {
    val _ = Try {
      inputStream.close()
    }
  }
}
